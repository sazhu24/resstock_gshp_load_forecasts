# ============================================================
# 03_analyze_results.R
#
# Purpose:
#   Analyze hourly results and generate figures and tables.
# ============================================================

library(tidyverse)
library(fs)
library(glue)

# ----------------------------
# 0) Configs
# ----------------------------
source("00_config.R")

# Change this to switch which scenario is processed
#active_scenario <- "scenario1"

# Use centralized config values
cfg <- list(
  summer_peak_day   = summer_peak_day,
  summer_peak_label = summer_peak_label,
  winter_peak_day   = winter_peak_day,
  winter_peak_label = winter_peak_label
)

# ----------------------------
# Helper: read scenario results
# ----------------------------
get_scenario <- function(
    scenario_name,
    label,
    file_name = "combined_8760.csv",
    root_dir
) {
  file_path <- fs::path(root_dir, scenario_name, "results", file_name)
  
  if (!fs::file_exists(file_path)) {
    stop(glue("Results file not found for {scenario_name}:\n{file_path}"),
         call. = FALSE)
  }
  
  df <- readr::read_csv(file_path, show_col_types = FALSE)
  
  list(
    scenario = scenario_name,
    label = label,
    path = file_path,
    df = df
  )
}

# ----------------------------
# Scenario map
# ----------------------------
scenario_map <- tibble::tribble(
  ~scenario_name, ~label,
  "scenario1", "Electric Furnace + Central AC",
  "scenario2", "Gas Furnace + Central AC",
  "scenario3", "Gas Furnace + Central AC",
  "scenario4", "Any Furnace + Central AC"
)

scenarios <- purrr::pmap(
  scenario_map,
  ~ get_scenario(
    scenario_name = ..1,
    label = ..2,
    file_name = "combined_8760.csv",
    root_dir = root_dir
  )
)
names(scenarios) <- scenario_map$scenario_name

# ----------------------------
# Select active scenario
# ----------------------------
hourly_load_data <- scenarios[[active_scenario]]$df
equipment <- scenarios[[active_scenario]]$label

results_dir <- fs::path(root_dir, active_scenario, "results")
fs::dir_create(results_dir)

# ----------------------------
# Annual energy consumption
# ----------------------------
annual_energy_compare <- hourly_load_data %>%
  summarize(
    elec_heat_base   = sum(all_heating_electricity_mwh_base, na.rm = TRUE),
    elec_cool_base   = sum(all_cooling_electricity_mwh_base, na.rm = TRUE),
    gas_heat_base    = sum(gas_heating_mwh_base, na.rm = TRUE),
    
    elec_heat_scaled = sum(heating_mwh_scaled, na.rm = TRUE),
    elec_cool_scaled = sum(cooling_mwh_scaled, na.rm = TRUE),
    gas_heat_scaled  = sum(gas_heating_mwh_scaled, na.rm = TRUE),
    
    elec_heat_upg    = sum(all_heating_electricity_mwh_upg, na.rm = TRUE),
    elec_cool_upg    = sum(all_cooling_electricity_mwh_upg, na.rm = TRUE),
    gas_heat_upg     = sum(gas_heating_mwh_upg, na.rm = TRUE)
  ) %>%
  pivot_longer(everything(), names_to = "metric", values_to = "mwh") %>%
  separate(metric, into = c("fuel_enduse", "scenario"), sep = "_(?=[^_]+$)") %>%
  mutate(
    scenario = recode(
      scenario,
      base   = "Baseline",
      scaled = "Baseline (Scaled Up)",
      upg    = "Upgrade"
    ),
    fuel_enduse = recode(
      fuel_enduse,
      elec_heat = "elec_heating_mwh",
      elec_cool = "elec_cooling_mwh",
      gas_heat  = "gas_heating_mwh"
    )
  ) %>%
  pivot_wider(names_from = fuel_enduse, values_from = mwh) %>%
  arrange(factor(scenario,
                 levels = c("Baseline", "Baseline (Scaled Up)", "Upgrade")))

# ----------------------------
# Plot helpers
# ----------------------------
theme_sz <- function() {
  theme(
    axis.title.y = element_text(vjust = 2, size = 14),
    axis.title.x = element_text(vjust = -0.5, size = 14),
    axis.text = element_text(size = 14),
    text = element_text(family = "Jost"),
    legend.text = element_text(size = 13),
    legend.title = element_text(size = 14, family = "Jost Medium"),
    plot.margin = unit(c(.6, .7, .4, .6), "cm"),
    plot.title = element_text(size = 14, family = "Jost Medium"),
    plot.subtitle = element_text(size = 12, vjust = 2),
    panel.background = element_rect(fill = "white"),
    panel.grid = element_line(colour = "#d1d1d1", linewidth = 0.5),
    panel.grid.major = element_line(colour = "#d1d1d1", linewidth = 0.2)
  )
}

plot_line <- function(df, x, y_cols, labels, title, subtitle = NULL, y_lab) {
  stopifnot(length(y_cols) == length(labels))
  
  df_long <- df %>%
    select({{ x }}, all_of(y_cols)) %>%
    pivot_longer(all_of(y_cols), names_to = "series", values_to = "value") %>%
    mutate(series = factor(series, levels = y_cols, labels = labels))
  
  ggplot(df_long, aes(x = {{ x }}, y = value, color = series)) +
    geom_line(linewidth = 1) +
    labs(title = title, subtitle = subtitle, x = NULL, y = y_lab, color = NULL) +
    scale_color_manual(values = c("#7FD0F1", "#1C7FAC", "#F0AC4B", "red")) +
    theme_sz() +
    theme(legend.position = "bottom")
}

# ----------------------------
# Summer peak day plot
# ----------------------------
summer_peak <- hourly_load_data %>%
  filter(str_starts(as.character(time_hr), cfg$summer_peak_day)) %>%
  transmute(
    time_hr,
    cooling_kw_base   = all_cooling_electricity_mwh_base * 1000,
    cooling_kw_upg    = all_cooling_electricity_mwh_upg  * 1000,
    cooling_kw_scaled = cooling_mwh_scaled * 1000
  )

p <- plot_line(
  summer_peak,
  x = time_hr,
  y_cols = c("cooling_kw_base", "cooling_kw_scaled", "cooling_kw_upg"),
  labels = c("Baseline AC (Unscaled)", "Baseline AC (Scaled Up)", "GSHP Cooling"),
  title = glue("Summer Peak Day Hourly Cooling Load ({equipment})"),
  subtitle = cfg$summer_peak_label,
  y_lab = "kW"
) +
  scale_x_datetime(date_labels = "%H:%M", date_breaks = "4 hours")

p

# ----------------------------
# Save outputs
# ----------------------------
ggsave(
  fs::path(results_dir, glue("summer_peak_cooling_{active_scenario}.png")),
  p,
  width = 9,
  height = 5,
  units = "in",
  dpi = 300
)

write_csv(
  annual_energy_compare,
  fs::path(results_dir, "annual_energy_consumption.csv")
)

write_csv(
  summer_peak %>% mutate(time_hr = as.character(time_hr)),
  fs::path(results_dir, "summer_peak_day.csv")
)
