# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)

## Custom Project

### Dataset
The dataset for my custom project includes reference metrics and current metrics for a coffee store. They are .csv format and includes fields like orders, complaints, avg_wait_time_ms .

### Signals
The primary signals used are numeric metrics to identify the drift. Signals were created to calculate the absolute difference between the current and reference metrics. Percentage changes between the current and the reference metrics were calculated to understand the pattern of drift. A drift flag was added , which is a binary indicator based on the Threshold set. The flag is triggered when the current metrics do not meet the threshold metrics.

### Experiments
Initially I lowered the existing threshold to see the relative change in the Threshold flag. I added percentage recipes to calcute the difference between current and reference metrics .I added a percentage drift threshold flag to detect changes easily. I also experimented by adding more fields to the existing dataframe

### Results
When a threshold was set to lower metrics , those thresholds were not flagged. When a higher threshold was set the metrics were flagged correctly. The percentage drift was a faster way to say the impact of drift and was successfully added to the existing data frame.

### Interpretation
the drift detection provides a clear and actionable way to monitor data metrics and manage the business from falling into huge risks by prompltly identifying drifts in the initial stages of difference.
