# GitHub Repository for the paper "Tracking Elusive and Shifting Identities of Global Fishing fleet" (in review)

This repo is to support the analyses provided in the paper (in review) entitled "Tracking Elusive and Shifting Identities of Global Fishing Fleet."

## Setup

### Running scripts (TO BE UPDATED)

1. Clone this repo by `git clone https://github.com/GlobalFishingWatch/paper-tracking-vessel-identity` 
2. You have all analysis scripts organized by theme under the `scripts` folder.
3. Follow the instructions in each folder.

### Environments (TO BE UPDATED)

1. For Python scripts, run `pip install -e .` to install the necessary libraries. This will create a folder titled `<module>.egg-info` that will allow you to access the code within your `<module>` folder from outside of that folder by doing `import <module>` without any need to use paths.
2. For R scripts, the R version is ... 


## Structure and descriptions

To learn more about this structure and how it is meant to be used throughout different stages of the project, see [this presentation](https://docs.google.com/presentation/d/1E51s4VhcLzCwN_v_yeOpaGLtNEF5fcZanlO1lRsmGiw/edit?usp=sharing).

    |- README.md		<- Top-level README on how to use this repo
    |- data			    <- Data files. Create subfolders as necessary.
    |- docs			    <- Documentation.
    |- scripts		    <- Regular python and R files. These may use code from <module> but are not
    |				       notebooks. A good place for scripts that run data pipelines, train models, etc.
    |- outputs		    <- Models results, static reports, etc. Create  additional subfolders as necessary.
    |    |- figures	    <- Create versioned figures folders if desired.
    |- queries		    <- SQL files (.sql, .jinja2, etc). Create subfolders as necessary. When creating
    |				       new tables in BigQuery, be sure to use a schema with good field descriptions.
    |- setup.cfg		<- Needed for pip install of repo.
    |- setup.py		    <- Needed for pip install of repo.
    |- tests		    <- Unit tests.
    |- .gitignore	    <- Modify as necessary.
    |- .pre-commit-config.yaml	    <- Style linting using `pre-commit library. Must run `pre-commit
    |                                  install` once to turn on commit hook. Modify as necessary.
