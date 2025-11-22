This repository contains my assessment implementation for CN7021 – Advanced Software Engineering.
Based on the original open-source project: https://github.com/subash0719/Paris-Olympics-Data-Engineering-Project
I will implement improvements, automation, documentation, and full workflow execution for my assessment.
Paris Olympics Data Engineering Project – Workflow Summary

This project implements a complete end-to-end data engineering pipeline for Paris Olympics datasets using PySpark, Python scripts, and data visualisation.
The workflow follows a Bronze → Silver → Gold architecture, with the final output saved as images and Parquet tables.

1. Bronze Layer – Raw Data Ingestion

The raw datasets (athletes, coaches, events, nocs) were ingested into the bronze folder.

Data stayed in its original form (no cleaning/modifications).

Stored using Parquet format.

2. Silver Layer – Data Cleaning & Standardisation

Created script: Script/silver_transform.py

Tasks performed:

Loaded raw parquet files from Bronze.

Removed duplicate rows.

Filled missing values with "Unknown".

Ensured clean, consistent, standardised data.

Saved outputs to silver folder in parquet format.

Silver tables created:

athletes

coaches

events

nocs

3. Gold Layer – Business-Level Aggregations

Created script: Script/gold_transform.py

Gold transformations include:

Athletes per Country

Coaches per Country

Events per Sport

Each transformation was:

Aggregated using PySpark.

Ordered for clarity.

Saved as parquet in the gold folder.

Gold tables created:

athletes_per_country

coaches_per_country

events_per_sport

4. Visualisation Layer – Saved as PNG Images

Created script: Script/visualize_olympics.py

Instead of interactive plotting, static PNG charts were generated.

Visuals:

Athletes per Country

Coaches per Country

Events per Sport

All plot images saved to visualizations/ folder.

5. GitHub Version Control

All new files (Bronze/Silver/Gold data, scripts, visualization outputs) were added.

Commit included pipeline scripts and generated datasets.

Successfully pushed to GitHub as the final submission.

Final Output Includes

ETL pipeline scripts
silver_transform.py, gold_transform.py, visualize_olympics.py

Bronze/Silver/Gold data layers

7 PNG visualisation files 

A fully working Paris Olympics data engineering workflow
