"""
case_drift_detector.py - Project script (example).

Author: Rucu Sethu
Date: 2026-04

Reference and Current Coffee Shop Operations Data

- Data is taken from a coffee shop measured during two different periods.
- The reference data represents slow season (Jan-Feb) behavior.
- The current data represents busy season (Dec) behavior.
- Each row represents one day of coffee shop operations.

- Each CSV file includes these columns:
  - orders: number of customer orders handled
  - complaints: number of customer complaints
  - avg_wait_time_ms: average customer wait time in milliseconds


Purpose

- Read reference and current coffee shop data from CSV files.
- Compare the two datasets using simple summary statistics.
- Detect meaningful changes in average operations behavior.
- Provide a simple baseline comparison approach that supports drift detection.
- Save the comparison summary as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- What does "normal" coffee shop operations look like in slow season?
- How can we compare busy season measurements to a slow season baseline?
- When does a difference become large enough to indicate meaningful change?

Paths (relative to repo root)

    INPUT FILE: data/reference_metrics_coffee.csv
    INPUT FILE: data/current_metrics_coffee.csv
    OUTPUT FILE: artifacts/drift_summary_coffee.csv

Terminal command to run this file from the root project folder

uv run python -m cintel.coffee_drift_detector


OBS:
  Don't edit this file - it should remain a working example.
  Use as much of this code as you can when creating your own pipeline script,
  and change the comparison logic and thresholds as needed for your project.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

REFERENCE_FILE: Final[Path] = DATA_DIR / "reference_metrics_coffee.csv"
CURRENT_FILE: Final[Path] = DATA_DIR / "current_metrics_coffee.csv"

OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_coffee.csv"
SUMMARY_LONG_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_long_coffee.csv"

# === DEFINE THRESHOLDS ===

# Analysts need to know their data and
# choose thresholds that make sense for their specific use case.

# Review the reference metrics to understand typical values
# and variability before setting thresholds.

# In this example, we compare current metrics to a reference period
# and flag drift when the difference exceeds these thresholds:

ORDERS_DRIFT_THRESHOLD: Final[float] = 30.0
COMPLAINTS_DRIFT_THRESHOLD: Final[float] = 3.0
WAIT_TIME_DRIFT_THRESHOLD: Final[float] = 800.0
# Percentage drift thresholds
ORDERS_DRIFT_PCT_THRESHOLD: Final[float] = 20.0
COMPLAINTS_DRIFT_PCT_THRESHOLD: Final[float] = 50.0
WAIT_TIME_DRIFT_PCT_THRESHOLD: Final[float] = 20.0


# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "REFERENCE_FILE", REFERENCE_FILE)
    log_path(LOG, "CURRENT_FILE", CURRENT_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure the artifacts folder exists.
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ REFERENCE AND CURRENT CSV INTO DATAFRAMES
    # ----------------------------------------------------
    reference_df = pl.read_csv(REFERENCE_FILE)
    current_df = pl.read_csv(CURRENT_FILE)

    LOG.info(f"Loaded {reference_df.height} reference records")
    LOG.info(f"Loaded {current_df.height} current records")

    # ----------------------------------------------------
    # STEP 2: CALCULATE AVERAGE METRICS FOR EACH PERIOD
    # ----------------------------------------------------
    # Summarize each dataset using average values so we can
    # compare slow season behavior to busy season behavior.

    reference_summary_df = reference_df.select(
        [
            pl.col("orders").mean().alias("reference_avg_orders"),
            pl.col("complaints").mean().alias("reference_avg_complaints"),
            pl.col("avg_wait_time_ms").mean().alias("reference_avg_wait_time_ms"),
        ]
    )

    current_summary_df = current_df.select(
        [
            pl.col("orders").mean().alias("current_avg_orders"),
            pl.col("complaints").mean().alias("current_avg_complaints"),
            pl.col("avg_wait_time_ms").mean().alias("current_avg_wait_time_ms"),
        ]
    )

    # ----------------------------------------------------
    # STEP 3: COMBINE THE TWO ONE-ROW SUMMARY TABLES
    # ----------------------------------------------------
    # Each summary table has one row.
    #
    # reference_summary_df:
    #   reference_avg_orders
    #   reference_avg_complaints
    #   reference_avg_wait_time
    #
    # current_summary_df:
    #   current_avg_orders
    #   current_avg_complaints
    #   current_avg_wait_time
    #
    # We combine them horizontally so both sets of values
    # appear side-by-side in a single row using the
    # concatenate function (pl.concat).
    #
    # This makes it easy to calculate:
    #   current value - reference value

    combined_df: pl.DataFrame = pl.concat(
        [reference_summary_df, current_summary_df],
        how="horizontal",
    )

    # ----------------------------------------------------
    # STEP 4: DEFINE DIFFERENCE RECIPES
    # ----------------------------------------------------
    # A difference recipe calculates:
    #
    #     current average - reference average
    #
    # Positive values mean the current period is larger.
    # Negative values mean the current period is smaller.

    orders_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_orders") - pl.col("reference_avg_orders"))
        .round(2)
        .alias("orders_mean_difference")
    )

    complaints_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_complaints") - pl.col("reference_avg_complaints"))
        .round(2)
        .alias("complaints_mean_difference")
    )

    wait_time_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_wait_time_ms") - pl.col("reference_avg_wait_time_ms"))
        .round(2)
        .alias("wait_time_mean_difference_ms")
    )

    # ----------------------------------------------------
    # STEP 4.1: APPLY THE DIFFERENCE RECIPES TO EXPAND THE DATAFRAME
    # ----------------------------------------------------
    drift_df: pl.DataFrame = combined_df.with_columns(
        [
            orders_mean_difference_recipe,
            complaints_mean_difference_recipe,
            wait_time_mean_difference_recipe,
        ]
    )
    # ----------------------------------------------------
    # STEP 5: DEFINE DRIFT FLAG RECIPES
    # ----------------------------------------------------
    # A drift flag recipe checks whether the absolute size
    # of the difference exceeds a threshold.
    #
    # We use abs() because either direction may matter:
    # - much higher than reference
    # - much lower than reference

    orders_is_drifting_flag_recipe: pl.Expr = (
        pl.col("orders_mean_difference").abs() > ORDERS_DRIFT_THRESHOLD
    ).alias("orders_is_drifting_flag")

    complaints_is_drifting_flag_recipe: pl.Expr = (
        pl.col("complaints_mean_difference").abs() > COMPLAINTS_DRIFT_THRESHOLD
    ).alias("complaints_is_drifting_flag")

    wait_time_drifting_flag_recipe: pl.Expr = (
        pl.col("wait_time_mean_difference_ms").abs() > WAIT_TIME_DRIFT_THRESHOLD
    ).alias("wait_time_is_drifting_flag")

    # --------------------------------------------------------
    # MODIFICATION 5.1A: PERCENTAGE DRIFT CALCULAIONS
    # -------------------------------------------------------
    drift_df = drift_df.with_columns(
        [
            ((pl.col("orders_mean_difference") / pl.col("reference_avg_orders")) * 100)
            .round(2)
            .alias("orders_pct_drift"),
            (
                (
                    pl.col("complaints_mean_difference")
                    / pl.col("reference_avg_complaints")
                )
                * 100
            )
            .round(2)
            .alias("complaints_pct_drift"),
            (
                (
                    pl.col("wait_time_mean_difference_ms")
                    / pl.col("reference_avg_wait_time_ms")
                )
                * 100
            )
            .round(2)
            .alias("wait_time_pct_drift"),
        ]
    )
    # ----------------------------------------------------
    # STEP 5.1B: APPLY THE DRIFT FLAG RECIPES TO EXPAND THE DATAFRAME
    # ----------------------------------------------------
    drift_df = drift_df.with_columns(
        [
            orders_is_drifting_flag_recipe,
            complaints_is_drifting_flag_recipe,
            wait_time_drifting_flag_recipe,
            # -------------------------------------------------------
            # MODIFICATION 2: ADDING PERCENTAGE DRIFT FLAGS FOR THRESHOLD TO SAME DF
            # --------------------------------------------------------
            (pl.col("orders_pct_drift").abs() > ORDERS_DRIFT_PCT_THRESHOLD).alias(
                "orders_pct_drift_flag"
            ),
            (
                pl.col("complaints_pct_drift").abs() > COMPLAINTS_DRIFT_PCT_THRESHOLD
            ).alias("complaints_pct_drift_flag"),
            (pl.col("wait_time_pct_drift").abs() > WAIT_TIME_DRIFT_PCT_THRESHOLD).alias(
                "wait_time_pct_drift_flag"
            ),
        ]
    )

    LOG.info("Calculated summary differences , percentage drift and drift flags")

    # ------ ----------------------------------------------
    # STEP 6: SAVE THE FLAT DRIFT SUMMARY AS AN ARTIFACT
    # ----------------------------------------------------
    drift_df.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote drift summary file: {OUTPUT_FILE}")

    # Take a look at the summary dataframe.
    # Lots of columns with one row of values.
    LOG.info("Drift summary dataframe:")
    LOG.info(drift_df)
    LOG.info("Let's make that a bit nicer to read...")
    LOG.info("All remaining steps are about creating a nicer display.")

    # ----------------------------------------------------
    # OPTIONAL STEP 6.1: LOG THE SUMMARY ONE FIELD PER LINE
    # ----------------------------------------------------
    # drift_df has one row with many columns.
    # Convert that one row to a dictionary so we can log:
    # column_name: value

    # The Polars to_dicts() function returns a list of dictionaries, one per row.
    # the [0] gets the first (and only) dictionary from the list.
    # We often count starting at zero
    # because the first row is 0 away from the start of the dataframe.
    drift_summary_dict = drift_df.to_dicts()[0]

    LOG.info("========================")
    LOG.info("Drift Detection Process: ")
    LOG.info("========================")
    LOG.info("1. Summarize each period with means.")
    LOG.info("2. Compute difference of means.")
    LOG.info("3. Flag drift if absolute difference of means exceeds a threshold.")
    LOG.info("========================")

    LOG.info("Drift summary (one field per line):")
    for field_name, field_value in drift_summary_dict.items():
        LOG.info(f"{field_name}: {field_value}")

    # ----------------------------------------------------
    # OPTIONAL STEP 7: CREATE A LONG-FORM ARTIFACT FOR DISPLAY
    # ----------------------------------------------------
    # Create a second artifact with one field per row.
    # This is easier to read than a single very wide row.
    # We create a new dataframe with two columns:
    # - field_name: the name of the summary field
    # - field_value: the value of the summary field (converted to string for display)

    drift_summary_long_df = pl.DataFrame(
        {
            "field_name": list(drift_summary_dict.keys()),
            "field_value": [str(value) for value in drift_summary_dict.values()],
        }
    )
    # ----------------------------------------------------
    # OPTIONAL STEP 7.1: SAVE THE LONG-FORM DRIFT SUMMARY AS AN ARTIFACT
    # ----------------------------------------------------
    drift_summary_long_df.write_csv(SUMMARY_LONG_FILE)
    LOG.info(f"Wrote long summary file: {SUMMARY_LONG_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===
if __name__ == "__main__":
    main()
