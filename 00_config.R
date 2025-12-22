
# R/00_config.R
# Global configuration shared across scripts

# Root directory for project file and outputs
root_dir <- "C:/Users/63526/OneDrive - ICF/VA GREC/resstock_gshp_load_forecasts"
stopifnot(dir.exists(root_dir))

# Sampling controls
sample_size <- 100
seed <- 123
