### Overview
This repo contains the necessary scripts and queries to generate the identity stitch data used to create vessel_record_id for the elusive identity paper

### File Descriptions

*identity_stitcher_dataset.py*
- A python file that processes the core identity data and generates the vessel_record_id used to track identity changes of a vessel hull.

*sql files*
- SQL queries to produce staging/final data and put them as BigQuery tables. Please see each file for a description of what it does.

*R files*
- These R scripts generate chord diagrams of flag changes in various ports depicted in the paper.