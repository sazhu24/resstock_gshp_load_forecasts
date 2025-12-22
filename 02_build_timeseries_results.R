# ============================================================
# 02_build_timeseries_results.R
#
# Purpose:
#   Download ResStock timeseries for sampled buildings,
#   aggregate to hourly, and compare baseline vs GSHP.
# ============================================================

library(tidyverse)
library(arrow)
library(lubridate)
library(glue)
library(fs)
library(conflicted)

conflicted::conflict_prefer("filter", "dplyr")
conflicted::conflict_prefer("summarize", "dplyr")

# ----------------------------
# 0) Config
# ----------------------------
source("00_config.R")

cfg <- list(
  state = "VA",
  timezone = "America/New_York",
  base_url = "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/timeseries_individual_buildings/by_state/",
  cache_dir = path(root_dir, "ResStock", "timeseries"),
  out_dir   = path(root_dir, "ResStock"),
  upgrade_baseline = 0,
  upgrade_gshp = 5,
  
  summer_peak_day   = "2018-07-02",
  summer_peak_hour  = "2018-07-02 16:00:00",
  summer_peak_label = "July 2, 4PM",
  
  winter_peak_day   = "2018-01-07",
  winter_peak_hour  = "2018-01-07 07:00:00",
  winter_peak_label = "Jan 7, 7AM"
)

dir_create(cfg$cache_dir)
dir_create(cfg$out_dir)

# ----------------------------
# Choose scenario to run
# ----------------------------
active_scenario <- "scenario2"  # <--- change this


# check if output file exists and prompt user to confirm override
out_file <- path(root_dir, "ResStock", active_scenario, "results", "combined_8760.csv")

if (fs::file_exists(out_file)) {
  msg <- glue(
    "Output file already exists:\n",
    "{out_file}\n\n",
    "Choose how to proceed:\n",
    "  [y] Continue and overwrite the file\n",
    "  [n] Stop execution (recommended)\n"
  )
  
  if (interactive()) {
    choice <- tolower(readline(msg))
    
    if (!choice %in% c("y", "yes")) {
      stop(glue("Execution stopped. Existing file was not overwritten:\n{out_file}"),
           call. = FALSE)
    }
    
    message("Overwriting existing file and continuing...")
    
  } else {
    stop(
      glue("Output file already exists: {out_file}\n",
           "Run interactively to confirm overwrite."),
      call. = FALSE
    )
  }
}

ids_path <- path(root_dir, "ResStock", active_scenario, "bldg_ids", "applicable_buildings.csv")
if (!file_exists(ids_path)) {
  stop(glue("Missing building sample file: {ids_path}. Run 01_select_building_samples.R first."))
}

# ----------------------------
# 1) Helpers: paths + download + read
# ----------------------------
parquet_url <- function(building_id, upgrade, state = cfg$state, base_url = cfg$base_url) {
  glue("{base_url}upgrade%3D{upgrade}/state%3D{state}/{building_id}-{upgrade}.parquet")
}

parquet_local <- function(building_id, upgrade, cache_dir = cfg$cache_dir) {
  path(cache_dir, glue("{building_id}-{upgrade}.parquet"))
}

download_if_needed <- function(url, dest) {
  if (!file_exists(dest)) {
    dir_create(path_dir(dest))
    download.file(url, destfile = dest, mode = "wb", quiet = TRUE)
  }
  dest
}

read_one_building <- function(building_id, upgrade) {
  url  <- parquet_url(building_id, upgrade)
  dest <- parquet_local(building_id, upgrade)
  download_if_needed(url, dest)
  
  read_parquet(dest) %>%
    mutate(bldg_id = as.integer(building_id), upgrade = upgrade)
}

# ----------------------------
# 2) Pull timeseries
# ----------------------------
get_timeseries <- function(buildings_df, upgrade) {
  stopifnot("bldg_id" %in% names(buildings_df))
  
  ids <- buildings_df %>%
    distinct(bldg_id) %>%
    mutate(bldg_id = as.integer(bldg_id)) %>%
    pull(bldg_id)
  
  purrr::map_dfr(ids, function(id) {
    tryCatch(
      read_one_building(id, upgrade),
      error = function(e) {
        message(glue("FAILED building_id={id}, upgrade={upgrade}: {e$message}"))
        NULL
      }
    )
  })
  
  if (i %% 10 == 0 || i == n_total) {
    message(glue("Loaded {i}/{n_total} buildings (upgrade {upgrade})"))
  }
  
}

# ----------------------------
# 3) Summarize 15-min to hourly, averaged across N buildings
# ----------------------------
summarize_hourly <- function(ts_df, n_buildings, type_label, timezone = cfg$timezone) {
  
  heat_cols <- names(ts_df) %>% stringr::str_subset("^out\\.electricity\\.heating\\..*energy_consumption$")
  cool_cols <- names(ts_df) %>% stringr::str_subset("^out\\.electricity\\.cooling\\..*energy_consumption$")
  
  ts_df %>%
    mutate(
      type = type_label,
      
      time = as.POSIXct(timestamp, format = "%Y-%m-%d %H", tz = timezone),
      
      # Shift forward 5 hours to convert from UCT
      time = time + hours(5),
      
      # Data is "hour ending" so shift back by 5 min so floor_date groups correctly.
      time = time - minutes(5),
      
      time_hr = floor_date(time, unit = "hour"),
      
      year  = year(time_hr),
      month = month(time_hr),
      day   = day(time_hr),
      hour  = hour(time_hr),
      
      all_heating_kwh = if (length(heat_cols) > 0) rowSums(pick(all_of(heat_cols)), na.rm = TRUE) else 0,
      all_cooling_kwh = if (length(cool_cols) > 0) rowSums(pick(all_of(cool_cols)), na.rm = TRUE) else 0,
      
      # Convert kWh → MWh and average across buildings
      across(ends_with("energy_consumption"), ~ .x / 1000 / n_buildings),
      all_heating_kwh = all_heating_kwh / 1000 / n_buildings,
      all_cooling_kwh = all_cooling_kwh / 1000 / n_buildings
    ) %>%
    group_by(type, year, month, day, hour, time_hr) %>%
    summarize(
      interval_count = n(),
      total_electricity_mwh       = sum(out.electricity.net.energy_consumption, na.rm = TRUE),
      all_heating_electricity_mwh = sum(all_heating_kwh, na.rm = TRUE),
      all_cooling_electricity_mwh = sum(all_cooling_kwh, na.rm = TRUE),
      heating_load_kbtu           = sum(out.load.heating.energy_delivered.kbtu, na.rm = TRUE),
      cooling_load_kbtu           = sum(out.load.cooling.energy_delivered.kbtu, na.rm = TRUE),
      gas_total_mwh               = sum(out.natural_gas.total.energy_consumption, na.rm = TRUE),
      gas_heating_mwh             = sum(out.natural_gas.heating.energy_consumption, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(time_hr)
}

# ----------------------------
# 4) Compare baseline vs upgrade (wide + savings + "scaled")
# ----------------------------
compare_scenarios <- function(df_base, df_upg) {
  df_base_w <- df_base %>%
    select(-type) %>%
    rename_with(~ paste0(.x, "_base"),
                c(total_electricity_mwh, all_heating_electricity_mwh, all_cooling_electricity_mwh,
                  heating_load_kbtu, cooling_load_kbtu, gas_total_mwh, gas_heating_mwh))
  
  df_upg_w <- df_upg %>%
    select(-type) %>%
    rename_with(~ paste0(.x, "_upg"),
                c(total_electricity_mwh, all_heating_electricity_mwh, all_cooling_electricity_mwh,
                  heating_load_kbtu, cooling_load_kbtu, gas_total_mwh, gas_heating_mwh))
  
  df_base_w %>%
    left_join(df_upg_w, by = c("year", "month", "day", "hour", "time_hr")) %>%
    mutate(
      total_electricity_savings   = total_electricity_mwh_base - total_electricity_mwh_upg,
      heating_electricity_savings = all_heating_electricity_mwh_base - all_heating_electricity_mwh_upg,
      cooling_electricity_savings = all_cooling_electricity_mwh_base - all_cooling_electricity_mwh_upg,
      
      gas_total_savings   = gas_total_mwh_base - gas_total_mwh_upg,
      gas_heating_savings = gas_heating_mwh_base - gas_heating_mwh_upg,
      
      cooling_load_ratio = if_else(cooling_load_kbtu_base == 0, 0, cooling_load_kbtu_upg / cooling_load_kbtu_base),
      heating_load_ratio = if_else(heating_load_kbtu_base == 0, 0, heating_load_kbtu_upg / heating_load_kbtu_base),
      
      nonthermal_mwh = total_electricity_mwh_base -
        (all_heating_electricity_mwh_base + all_cooling_electricity_mwh_base),
      
      nonthermal_gas = gas_total_mwh_base - gas_heating_mwh_base,
      
      cooling_mwh_scaled = all_cooling_electricity_mwh_base * cooling_load_ratio,
      heating_mwh_scaled = all_heating_electricity_mwh_base * heating_load_ratio,
      
      cooling_mwh_savings_scaled = cooling_mwh_scaled - all_cooling_electricity_mwh_upg,
      heating_mwh_savings_scaled = heating_mwh_scaled - all_heating_electricity_mwh_upg,
      
      total_mwh_scaled         = heating_mwh_scaled + cooling_mwh_scaled + nonthermal_mwh,
      total_mwh_savings_scaled = heating_mwh_savings_scaled + cooling_mwh_savings_scaled,
      
      gas_heating_mwh_scaled = gas_heating_mwh_base * heating_load_ratio,
      gas_total_mwh_scaled   = nonthermal_gas + gas_heating_mwh_scaled
    ) %>%
    relocate(time_hr, .before = 1)
}

# ----------------------------
# 6) Run pipeline
# ----------------------------
create_sample_series <- function(scenario, out_name = "combined_8760") {
  
  # Read list of building ids from filtered sample
  applicable_buildings <- read_csv(
    path("ResStock", scenario, "bldg_ids", "applicable_buildings.csv"),
    show_col_types = FALSE
  )
  
  n_buildings <- nrow(applicable_buildings)
  
  # Pull raw timeseries
  ts_base <- get_timeseries(applicable_buildings, cfg$upgrade_baseline)
  ts_upg  <- get_timeseries(applicable_buildings, cfg$upgrade_gshp)
  
  # Hourly summaries
  hr_base <- summarize_hourly(ts_base, n_buildings, "Baseline")
  hr_upg  <- summarize_hourly(ts_upg,  n_buildings, "Upgrade")
  
  # Comparisons
  cmp <- compare_scenarios(hr_base, hr_upg)
  
  out_dir <- path("ResStock", scenario, "results")
  dir_create(out_dir)
  
  write_csv(
    cmp %>% mutate(time_hr = as.character(time_hr)),
    path(out_dir, glue("{out_name}.csv"))
  )
  
  cmp
}

# ----------------------------
# 7) Execute
# ----------------------------
cmp <- create_sample_series(active_scenario, out_name = "combined_8760")