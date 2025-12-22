# ResStock GSHP Load Forecasts

This project analyzes the potential impact of Ground Source Heat Pump (GSHP) retrofits on residential building energy loads in Virginia using NREL's ResStock dataset. The analysis compares baseline heating/cooling systems with GSHP upgrades across multiple building scenarios.

## Overview

The analysis workflow:
1. Identifies eligible residential buildings from ResStock metadata
2. Downloads and processes hourly timeseries energy data
3. Compares baseline vs. GSHP upgrade energy consumption
4. Generates visualizations and summary statistics

## Project Structure

```
├── 00_config.R                    # Global configuration settings
├── 01_select_building_samples.R   # Building selection and sampling
├── 02_build_timeseries_results.R  # Timeseries data processing
├── 03_analyze_results.R           # Analysis and visualization
├── data/                          # Input metadata files
│   ├── VA_baseline_metadata_and_annual_results.csv
│   └── VA_upgrade05_metadata_and_annual_results.csv
└── ResStock/                      # Output directory
    ├── scenario1/                 # Electric furnace (1970-1990s)
    ├── scenario2/                 # Gas furnace (1970-1990s)
    ├── scenario3/                 # Gas furnace (1990s)
    ├── scenario4/                 # Any furnace type
    ├── scenario5/                 # ...
    └── timeseries/               # Cached raw timeseries data
```

## Requirements

### R Packages
```r
tidyverse
arrow
lubridate
glue
fs
conflicted
```

Install with:
```r
install.packages(c("tidyverse", "arrow", "lubridate", "glue", "fs", "conflicted"))
```

## Usage

### Step 1: Configure

Edit [00_config.R](00_config.R) to configure the analysis:

**Required settings:**
- `root_dir`: Project directory path
- `active_scenario`: Which scenario to process (e.g., "scenario1", "scenario2")

**Optional settings:**
- `sample_size`: Number of buildings to sample per scenario (default: 100)
- `seed`: Random seed for reproducibility (default: 123)
- Peak day definitions (summer/winter dates and times)
- ResStock parameters (state, timezone, upgrade numbers)
- File paths for input data and outputs

**All scripts now read from this centralized config**, so you only need to change `active_scenario` once to switch between scenarios.

### Step 2: Select Building Samples

Run [01_select_building_samples.R](01_select_building_samples.R):
- Filters ResStock buildings by type, HVAC characteristics, and other criteria
- Draws random samples of eligible buildings for the active scenario
- Outputs: `ResStock/scenario*/bldg_ids/applicable_buildings.csv`

**Scenario Definitions:**
- **Scenario 1**: Electric furnace homes (1698-2678 sq ft, 1970s-1990s)
- **Scenario 2**: Natural gas furnace homes (1698-2678 sq ft, 1970s-1990s)
- **Scenario 3**: Natural gas furnace homes (1698-2678 sq ft, 1990s only)
- **Scenario 4**: Any furnace type (no filtering by size/vintage)
- **Scenario 5**: Gas furnace homes (1698-2678 sq ft, 1970s only)

All scenarios require single-family detached homes with ducts, central AC, and occupied status.

### Step 3: Build Timeseries Results

Run [02_build_timeseries_results.R](02_build_timeseries_results.R):
- Downloads hourly energy data from NREL's S3 bucket for the active scenario
- Processes baseline (Upgrade 0) and GSHP (Upgrade 5) timeseries
- Calculates scaled GSHP loads based on equipment sizing
- Identifies peak day loads (summer and winter)
- Outputs: `ResStock/scenario*/results/combined_8760.csv`

### Step 4: Analyze Results

Run [03_analyze_results.R](03_analyze_results.R):
- Compares annual energy consumption (baseline vs. GSHP) for the active scenario
- Generates hourly load profiles and peak day comparisons
- Creates visualizations

## Data Sources

### ResStock Metadata
- Source: NREL ResStock v2024 (AMY2018 Release 2)
- Location: `data/` directory
- Contains building characteristics and annual energy results for Virginia

### Timeseries Data
- Source: [OEDI Data Lake](https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/)
- Downloaded automatically by `02_build_timeseries_results.R`
- Cached locally in `ResStock/timeseries/`

## Key Outputs

### Building Samples
- `ResStock/scenario*/bldg_ids/applicable_buildings.csv`: Selected building IDs with metadata

### Energy Analysis
- `ResStock/scenario*/results/combined_8760.csv`: Hourly loads for all buildings
- `ResStock/scenario*/results/annual_energy_consumption.csv`: Annual totals comparison
- `ResStock/scenario*/results/summer_peak_day.csv`: Summer peak day analysis

## Peak Hours

- **Summer Peak**: July 2, 2018 at 4:00 PM
- **Winter Peak**: January 7, 2018 at 7:00 AM

## Notes

- Equipment sizing uses ton equivalents (1 ton = 12,000 BTU/h)
- GSHP upgrade (Upgrade 5) eliminates gas heating and replaces it with electric GSHP
- Scaling factors account for differences in GSHP vs. baseline equipment capacity


