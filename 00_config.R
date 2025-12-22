
# R/00_config.R
# Global configuration shared across scripts

# Root directory for project file and outputs
root_dir <- "C:/Users/63526/OneDrive - ICF/VA GREC/resstock_gshp_load_forecasts"
stopifnot(dir.exists(root_dir))

# ----------------------------
# Active scenario selection
# ----------------------------
# Change this to switch which scenario is processed
active_scenario <- "scenario1"

# ----------------------------
# Sampling controls
# ----------------------------
sample_size <- 100
seed <- 123

# ----------------------------
# ResStock data parameters
# ----------------------------
state <- "VA"
timezone <- "America/New_York"
upgrade_baseline <- 0
upgrade_gshp <- 5

# OEDI Data Lake URL
base_url <- "https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/timeseries_individual_buildings/by_state/"

# ----------------------------
# Peak day definitions
# ----------------------------
summer_peak_day   <- "2018-07-02"
summer_peak_hour  <- "2018-07-02 16:00:00"
summer_peak_label <- "July 2, 4PM"

winter_peak_day   <- "2018-01-07"
winter_peak_hour  <- "2018-01-07 07:00:00"
winter_peak_label <- "Jan 7, 7AM"

# ----------------------------
# File paths
# ----------------------------
baseline_csv <- file.path(root_dir, "data", "VA_baseline_metadata_and_annual_results.csv")
upgrade05_csv <- file.path(root_dir, "data", "VA_upgrade05_metadata_and_annual_results.csv")
cache_dir <- file.path(root_dir, "ResStock", "timeseries")
resstock_dir <- file.path(root_dir, "ResStock")
