### Overview
This repo contains the necessary scripts and queries to generate the core datasets for the elusive identity paper
The core data tables are also available at Global Fishing Watch's data download portal [here](https://globalfishingwatch.org/data-download/datasets/public-vessel-identity:v20230118)

### File Descriptions

*create_identity_dataset.py*
- A python file that processes collected registry data and generates the core data tables used for the elusive identity paper.

*sql files*
- SQL queries to produce staging/final data and put them as BigQuery tables. Please see each file for a description of what it does.

*csv, excel files*
- These files contain description texts for data tables.