# ============================================================
# 01_select_building_samples.R
#
# Purpose:
#   Identify eligible ResStock buildings and draw reproducible
#   samples for each scenario. Outputs building ID lists used
#   by downstream timeseries scripts.
# ============================================================

library(tidyverse)
library(glue)
library(conflicted)
library(fs)

conflicted::conflict_prefer("filter", "dplyr")
conflicted::conflict_prefer("summarize", "dplyr")

# ----------------------------
# 0) Configs
# ----------------------------

source("00_config.R")

# Change this to switch which scenario is processed
#active_scenario <- "scenario1"

# ----------------------------
# Scenario definitions
# ----------------------------
cfg_defaults <- list(
  root_dir   = root_dir,
  sample_size = sample_size,
  seed        = seed,
  
  building_type       = "Single-Family Detached",
  ducts_required      = "Yes",
  vacancy_required    = "Occupied",
  cooling_type_required = "Central AC"
)

# Merge defaults + scenario-specific overrides
merge_cfg <- function(...) modifyList(cfg_defaults, list(...))

cfgs <- list(
  scenario1 = merge_cfg(
    scenario = "scenario1",
    sqft_allowed    = c(1698, 2179, 2678),
    vintage_allowed = c("1990s", "1980s", "1970s"),
    hvac_efficiency = "Electric Furnace, 100% AFUE",
    heating_fuel    = "Electricity"
  ),
  
  scenario2 = merge_cfg(
    scenario = "scenario2",
    sqft_allowed    = c(1698, 2179, 2678),
    vintage_allowed = c("1990s", "1980s", "1970s"),
    hvac_efficiency = "Fuel Furnace, 80% AFUE",
    heating_fuel    = "Natural Gas"
  ),
  
  scenario3 = merge_cfg(
    scenario = "scenario3",
    sqft_allowed    = c(1698, 2179, 2678),
    vintage_allowed = c("1990s"),
    hvac_efficiency = "Fuel Furnace, 80% AFUE",
    heating_fuel    = "Natural Gas"
  ),
  
  scenario4 = merge_cfg(
    scenario = "scenario4"
    # no optional filters
  ),
  
  scenario5 = merge_cfg(
    scenario = "scenario5",
    sqft_allowed    = c(1698, 2179, 2678),
    vintage_allowed = c("1990s", "1980s", "1970s"),
    hvac_efficiency = "Fuel Furnace, 80% AFUE"
    # heating_fuel optional / omitted
  )
)

# ----------------------------
# Set which cfg to run
# ----------------------------
cfg <- cfgs[[active_scenario]]
stopifnot(!is.null(cfg))

# ----------------------------
# Paths (derived from cfg)
# ----------------------------
paths <- list(
  baseline_csv = fs::path(cfg$root_dir, "data", "VA_baseline_metadata_and_annual_results.csv"),
  u5_csv       = fs::path(cfg$root_dir, "data", "VA_upgrade05_metadata_and_annual_results.csv"),
  out_dir      = fs::path("ResStock", cfg$scenario, "bldg_ids"),
  out_path     = fs::path("ResStock", cfg$scenario, "bldg_ids", "applicable_buildings.csv")
)

fs::dir_create(paths$out_dir)

# ----------------------------
# 1) Helpers
# ----------------------------

as_bool <- function(x) {
  if (is.logical(x)) return(x)
  if (is.numeric(x)) return(x != 0)
  tolower(as.character(x)) %in% c("true", "t", "1", "yes", "y")
}

row_sum_if_present <- function(df, cols) {
  if (length(cols) == 0) return(rep(0, nrow(df)))
  rowSums(df[, cols, drop = FALSE], na.rm = TRUE)
}

cols_regex <- function(df, pattern) {
  names(df) %>% stringr::str_subset(pattern)
}

prep_baseline <- function(df, cfg) {
  heat_cols <- cols_regex(df, "^out\\.electricity\\.heating\\..*energy_consumption\\.kwh$")
  cool_cols <- cols_regex(df, "^out\\.electricity\\.cooling\\..*energy_consumption\\.kwh$")
  
  df %>%
    mutate(
      applicability = as_bool(applicability),
      
      heating_electricity_kwh = row_sum_if_present(., heat_cols),
      cooling_electricity_kwh = row_sum_if_present(., cool_cols),
      thermal_electricity_kwh = heating_electricity_kwh + cooling_electricity_kwh,
      nonthermal_electricity_kwh = out.electricity.total.energy_consumption.kwh - thermal_electricity_kwh,
      
      heating_tons = out.params.size_heating_system_primary_k_btu_h / 12,
      cooling_tons = out.params.size_cooling_system_primary_k_btu_h / 12,
      tons = pmax(heating_tons, cooling_tons)
    ) %>%
    filter(
      in.geometry_building_type_acs == cfg$building_type,
      applicability,
      in.hvac_has_ducts == cfg$ducts_required
    )
}

prep_upgrade5 <- function(df_u5) {
  df_u5 %>%
    transmute(
      bldg_id,
      applicability5 = as_bool(applicability),
      upgrade_tons_heating = out.params.size_heating_system_primary_k_btu_h / 12,
      upgrade_tons_cooling = out.params.size_cooling_system_primary_k_btu_h / 12
    )
}

# Helper: TRUE if cfg has a non-empty field
cfg_has <- function(cfg, name) {
  !is.null(cfg[[name]]) && length(cfg[[name]]) > 0 && !all(is.na(cfg[[name]]))
}

apply_sampling_filters <- function(df, cfg) {
  out <- df %>%
    filter(
      in.vacancy_status == cfg$vacancy_required,
      in.hvac_cooling_type == cfg$cooling_type_required
    )
  
  # Optional filters (only applied if present in cfg)
  if (cfg_has(cfg, "sqft_allowed")) {
    out <- out %>% filter(in.sqft %in% cfg$sqft_allowed)
  }
  
  if (cfg_has(cfg, "vintage_allowed")) {
    out <- out %>% filter(in.vintage %in% cfg$vintage_allowed)
  }
  
  if (cfg_has(cfg, "hvac_efficiency")) {
    out <- out %>% filter(in.hvac_heating_efficiency == cfg$hvac_efficiency)
  }
  
  if (cfg_has(cfg, "heating_fuel")) {
    out <- out %>% filter(in.heating_fuel == cfg$heating_fuel)
  }
  
  out
}

sample_ids <- function(df, n, seed) {
  n_avail <- nrow(df)
  if (n_avail == 0) stop("No rows available after filtering.")
  withr::with_seed(seed, {
    df %>% slice_sample(n = min(n, n_avail), replace = FALSE)
  })
}

# ----------------------------
# 2) Read inputs
# ----------------------------
if (!fs::file_exists(paths$baseline_csv)) stop(glue("Missing file: {paths$baseline_csv}"))
if (!fs::file_exists(paths$u5_csv)) stop(glue("Missing file: {paths$u5_csv}"))

rstock_base <- read_csv(paths$baseline_csv, show_col_types = FALSE)
rstock_u5 <- read_csv(paths$u5_csv, show_col_types = FALSE)

# ----------------------------
# 3) Build eligible pool (+ enforce upgrade5 applicability)
# ----------------------------
eligible <- prep_baseline(rstock_base, cfg) %>%
  left_join(prep_upgrade5(rstock_u5), by = "bldg_id") %>%
  filter(applicability5) %>%
  apply_sampling_filters(cfg)

# ----------------------------
# 4) Sample IDs (reproducible)
# ----------------------------
applicable_buildings <- sample_ids(eligible, cfg$sample_size, cfg$seed)

# ----------------------------
# 5) Write output (ONLY applicable_buildings)
# ----------------------------
write_csv(applicable_buildings, paths$out_path)

message(glue("Wrote {nrow(applicable_buildings)} buildings to: {paths$out_path}"))

# ----------------------------
# Get summary stats
# ----------------------------
applicable_buildings_avg <- applicable_buildings %>%
  #group_by(in.heating_fuel) |> 
  summarize(
    count = n(),
    base_tons_heating = mean(out.params.size_heating_system_primary_k_btu_h / 12, na.rm = TRUE),
    base_tons_cooling = mean(out.params.size_cooling_system_primary_k_btu_h / 12, na.rm = TRUE),
    upgrade_tons_heating = mean(upgrade_tons_heating, na.rm = TRUE),
    gas_heating = mean((out.natural_gas.heating.energy_consumption.kwh)/1000),
    propane_heating = mean((out.propane.heating.energy_consumption.kwh)/1000),
    fuel_oil_heating = mean((out.fuel_oil.heating.energy_consumption.kwh)/1000)
  )

write_csv(applicable_buildings_avg, glue('{paths$out_dir}/sample_average.csv'))
