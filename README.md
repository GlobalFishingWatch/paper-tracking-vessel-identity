[![DOI](https://zenodo.org/badge/441320150.svg)](https://zenodo.org/badge/latestdoi/441320150)
# GitHub Repository for the paper "Tracking Elusive and Shifting Identities of the Global Fishing fleet" (published in _Science Advances_ on January 18, 2023)

This repo is to support the analyses provided in the paper entitled ["Tracking Elusive and Shifting Identities of the Global Fishing Fleet"](science.org/doi/10.1126/sciadv.abp8200) published on January 18, 2023 in _Science Advances_ 

Paper citation: J. Park, J. Van Osdel, J. Turner, C. M. Farthing, N. A. Miller, H. L. Linder, G. Ortuño Crespo, G. Carmine, D. A. Kroodsma, Tracking elusive and shifting identities of the global fishing fleet. Sci. Adv. 9, eabp8200 (2023).

The core data used for this study are also available [here](https://globalfishingwatch.org/data-download/datasets/public-vessel-identity:v20230118) and the methodology is described in detail in the [Supplementary Materials](https://drive.google.com/file/d/1H-Fd0JfB5eDEKxn-h7sxH0Ci3eSIGg3F/view?usp=share_link) of the paper.  

## Setup

### Running scripts

1. Clone this repo by `git clone https://github.com/GlobalFishingWatch/paper-tracking-vessel-identity` 
2. You have all analysis scripts organized by theme under the `paper_tracking_vessel_identity` folder.
3. Follow the instructions in each folder.

### Environments

1. For Python scripts
  * Requirement: python >= 3.8.0 (see packages in detail radenv_videntity.yaml)
  * run `pip install -e .` to install the necessary packages. This will create a folder titled `<module>.egg-info` that will allow you to access the code within `paper_tracking_vessel_identity` folder from outside of that folder by doing `import <module>` without any need to use paths.
2. For R scripts, the tested R version is 3.6.3 


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
