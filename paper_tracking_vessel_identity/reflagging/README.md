### Overview
This repo contains the necessary scripts and queries to generate the reflagging summary data used for the elusive identity paper

### File Descriptions

*create_reflaggin_dataset.py*
- A python file that processes the core identity data and generates the flag change summary data table used to track reflagging behavior of vessels.

*sql files*
- SQL queries to produce staging/final data and put them as BigQuery tables. Please see each file for a description of what it does.

*map_reflagging_v20220701.py*
- A jupytext .py file that can be opened as a Jupyter notebook. This script produces figures related to reflagging in the elusive identity paper.