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

# Change this to switch which scenario is processed
#active_scenario <- "scenario1"

# Use centralized config values
cfg <- list(
  state = state,
  timezone = timezone,
  base_url = base_url,
  cache_dir = cache_dir,
  out_dir   = resstock_dir,
  upgrade_baseline = upgrade_baseline,
  upgrade_gshp = upgrade_gshp,
  
  summer_peak_day   = summer_peak_day,
  summer_peak_hour  = summer_peak_hour,
  summer_peak_label = summer_peak_label,
  
  winter_peak_day   = winter_peak_day,
  winter_peak_hour  = winter_peak_hour,
  winter_peak_label = winter_peak_label
)

# ----------------------------
# Paths
# ----------------------------
results_dir <- path("ResStock", active_scenario, "results")
out_path_summer <- path(results_dir, "all_buildings_summer_peak_hr.csv")
out_path_winter <- path(results_dir, "all_buildings_winter_peak_hr.csv")
out_path_combined_8760 <- path(results_dir, "all_buildings_winter_peak_hr.csv")

dir_create(results_dir, recurse = TRUE)
dir_create(cfg$cache_dir)
dir_create(cfg$out_dir)

rstock_base <- read_csv(baseline_csv)
rstock_u5 <- read_csv(upgrade05_csv)

ids_path <- path(root_dir, "ResStock", active_scenario, "bldg_ids", "applicable_buildings.csv")
if (!file_exists(ids_path)) {
  stop(glue("Missing building sample file: {ids_path}. Run 01_select_building_samples.R first."))
}

# ----------------------------
# Helpers: paths + download + read
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
# Pull timeseries
# ----------------------------
get_timeseries <- function(buildings_df, upgrade) {
  stopifnot("bldg_id" %in% names(buildings_df))
  
  ids <- buildings_df %>%
    distinct(bldg_id) %>%
    mutate(bldg_id = as.integer(bldg_id)) %>%
    pull(bldg_id)
  
  n_total <- length(ids)
  
  purrr::map_dfr(seq_along(ids), function(i) {
    id <- ids[[i]]
    
    out <- tryCatch(
      read_one_building(id, upgrade),
      error = function(e) {
        message(glue::glue("FAILED building_id={id}, upgrade={upgrade}: {e$message}"))
        NULL
      }
    )
    
    if (i %% 10 == 0 || i == n_total) {
      message(glue::glue("Loaded {i}/{n_total} buildings (upgrade {upgrade})"))
    }
    
    out
  })
}

# ----------------------------
# Summarize 15-min to hourly, averaged across N buildings
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
# Compare baseline vs upgrade (wide + savings + "scaled")
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
# Summarize timeseries to hourly by building
# ----------------------------
summarize_ts <- function(ts, prefix) {
  prefix <- rlang::as_string(prefix)
  
  ts %>%
    mutate(
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
      
      all_heating_kwh = rowSums(
        across(starts_with("out.electricity.heating") & ends_with("energy_consumption")),
        na.rm = TRUE
      ),
      all_cooling_kwh = rowSums(
        across(starts_with("out.electricity.cooling") & ends_with("energy_consumption")),
        na.rm = TRUE
      )
    ) %>%
    group_by(bldg_id, year, month, day, hour) %>%
    summarise(
      !!paste0(prefix, ".cooling.energy.kbtu.CP") :=
        sum(out.load.cooling.energy_delivered.kbtu, na.rm = TRUE),
      
      !!paste0(prefix, ".heating.energy.kbtu.CP") :=
        sum(out.load.heating.energy_delivered.kbtu, na.rm = TRUE),
      
      !!paste0(prefix, "_total_elec_kwh") :=
        sum(out.electricity.net.energy_consumption, na.rm = TRUE),
      
      !!paste0(prefix, "_cooling_total_elec_kwh") :=
        sum(all_cooling_kwh, na.rm = TRUE),
      
      !!paste0(prefix, "_cooling_elec_kwh") :=
        sum(out.electricity.cooling.energy_consumption, na.rm = TRUE),
      
      !!paste0(prefix, "_cooling_fans_pumps_elec_kwh") :=
        sum(out.electricity.cooling_fans_pumps.energy_consumption, na.rm = TRUE),
      
      !!paste0(prefix, "_heating_elec_kwh") :=
        sum(all_heating_kwh, na.rm = TRUE),
      
      !!paste0(prefix, "_heating_gas_kwh") :=
        sum(out.natural_gas.heating.energy_consumption, na.rm = TRUE),
      
      !!paste0(prefix, "_heating_propane_kwh") :=
        sum(out.propane.heating.energy_consumption, na.rm = TRUE),
      
      !!paste0(prefix, "_heating_fuel_oil_kwh") :=
        sum(out.fuel_oil.heating.energy_consumption, na.rm = TRUE),
      
      .groups = "drop"
    )
}

# ----------------------------
# Run pipeline
# ----------------------------
create_sample_series <- function(scenario, out_name = "combined_8760") {
  
  # Read list of building ids from filtered sample
  applicable_buildings <- readr::read_csv(
    fs::path("ResStock", scenario, "bldg_ids", "applicable_buildings.csv"),
    show_col_types = FALSE
  )
  
  stopifnot("bldg_id" %in% names(applicable_buildings))
  
  n_buildings <- applicable_buildings %>%
    dplyr::distinct(bldg_id) %>%
    nrow()
  
  # Pull raw timeseries
  ts_base <- get_timeseries(applicable_buildings, cfg$upgrade_baseline)
  ts_upg  <- get_timeseries(applicable_buildings, cfg$upgrade_gshp)
  
  # Hourly summaries
  hr_base <- summarize_hourly(ts_base, n_buildings, "Baseline")
  hr_upg  <- summarize_hourly(ts_upg,  n_buildings, "Upgrade")
  
  # Comparisons
  cmp <- compare_scenarios(hr_base, hr_upg) |> 
    mutate(time_hr = as.character(time_hr))
  
  # Timeseries by building
  b1 <- summarize_ts(ts_base, "baseline")
  u1 <- summarize_ts(ts_upg,  "upgrade")
  
  all_buildings <- b1 %>%
    left_join(u1, by = c("bldg_id", "year", "month", "day", "hour")) %>%
    mutate(
      time_hour = as.character(make_datetime(year, month, day, hour, tz = timezone))
    )
  
  # ----------------------------
  # Summer peak hour slice
  # ----------------------------
  all_buildings_summer_peak <- all_buildings %>%
    filter(time_hour == summer_peak_hour)
  
  # ----------------------------
  # Winter peak hour slice
  # ----------------------------
  all_buildings_winter_peak <- all_buildings %>%
    filter(time_hour == winter_peak_hour)
  
  # ----------------------------
  # Join with metadata
  # ----------------------------
  df <- rstock_base %>%
    select(
      bldg_id, in.sqft, in.ground_thermal_conductivity, in.vintage,
      in.heating_fuel, in.hvac_heating_efficiency, in.hvac_cooling_efficiency,
      in.hvac_cooling_partial_space_conditioning,
      in.hvac_has_ducts,
      out.unmet_hours.cooling.hour,
      out.unmet_hours.heating.hour,
      out.params.size_heating_system_primary_k_btu_h,
      out.params.size_cooling_system_primary_k_btu_h
    ) %>%
    left_join(
      rstock_u5 %>%
        select(bldg_id, out.params.size_heating_system_primary_k_btu_h) %>%
        rename(upgrade_heating_size = out.params.size_heating_system_primary_k_btu_h),
      by = "bldg_id"
    ) %>%
    rename(
      base_heating_size = out.params.size_heating_system_primary_k_btu_h,
      base_cooling_size = out.params.size_cooling_system_primary_k_btu_h
    ) %>%
    mutate(
      in.hvac_cooling_partial_space_conditioning =
        as.numeric(str_extract(in.hvac_cooling_partial_space_conditioning, "\\d+")) / 100,
      sqft_conditioned = in.sqft * in.hvac_cooling_partial_space_conditioning
    )
  
  df_summer_peak_hr <- df %>%
    inner_join(all_buildings_summer_peak, by = "bldg_id")
  
  df_winter_peak_hr <- df %>%
    inner_join(all_buildings_winter_peak, by = "bldg_id")
  
  write_csv(cmp, out_path_combined_8760)
  write_csv(df_summer_peak_hr, out_path_summer)
  write_csv(df_winter_peak_hr, out_path_winter)
  
  list(
    cmp = cmp,
    ts_base = ts_base,
    ts_upg = ts_upg,
    hr_base = hr_base,
    hr_upg = hr_upg,
    hr_base = hr_base,
    df_summer_peak_hr = df_summer_peak_hr,
    df_winter_peak_hr = df_winter_peak_hr,
    applicable_buildings = applicable_buildings
  )
}

# ----------------------------
# Execute
# ----------------------------
sample_series <- create_sample_series(active_scenario)

# cmp <- sample_series$cmp
# out_path_summer <- sample_series$df_summer_peak_hr
# out_path_winter  <- sample_series$out_path_winter
