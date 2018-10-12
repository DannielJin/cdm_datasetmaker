# cdm_datasetmaker

Generating datasets for time-series prediction tasks from the OMOP-CDM DB.
1. Target cohort & comparator cohort
2. List-List-List-List formatted seq_data
3. A tensor formatted as (BATCH_SIZE, MAX_TIME_STEPS, FEATURE_SIZE)
4. Datasets


## Installation

At /PATH/TO/THE/PACKAGE/FOLDER/:

```sh
pip3 install ./dist/*.whl
```
```sh
pip3 uninstall ./dist/*.whl -y
```

## Usage example

DB_connection.txt :
```
SERVER_IP = SERVER_IP:PORT #127.0.0.1:1433
USER = USERNAME #user
PASSWD = PASSWORD #passwd
CDM_DB = CDM_DB_NAME ##DB where OMOP_CDM data stored
RESULT_DB = RESULT_DB_NAME ##DB where you will store query_outputs
```

DS_PARAMS.txt:
```
DB_CONN_FILENAME = DB_connection.txt
TARGET_CODE = 316139  # target_concept_id(s) (e.g.) 111, 222, 333
EXCLUSION_CODE =  # exclusion_concept_id(s) (e.g.) 444, 555
INDEX_NUM = 1 
INDEX_AGE_OVER = 40
INDEX_AGE_UNDER = 85
OUTCOME_START = 2011-01-01
OUTCOME_END = 2013-12-31
MIN_TIME_STEP = 2
MAX_TIME_STEP = 30
FEATURE_OBS_START = 2003-01-01
FEATURE_OBS_END = 2010-12-31
M_FEAT_OBS_NUM = 1  # for matching
M_FEAT_OBS_UNIT = YEAR  # for matching
RATIO = 6  # matching ratio
WITH_CODE_FREQ = False  # using demoInfo+seqInfo for matching
CALIPER = 0.05  # for matching
METHOD = RANDOM # PSM, KNN, RNADOM
DX_ONLY = False # using Dx & Rx
TR_RATIO = 0.8
```

Main codes:
```
from cdm_datasetmaker import Get_datasets
datasets = Get_datasets(CONFIG_FOLDER_PATH = './CONFIG/',       #/PATH/TO/CONFIG/FOLDER/
                        DATA_FOLDER_PATH = './DATA/',           #/PATH/TO/DATA/FOLDER/ (save&load)
                        RESULT_FOLDER_PATH = './RESULT/',       #/PATH/TO/RESULT/FOLDER/ (logs)
                        PROJECT_NAME = 'PROJECT_DATASETS',      #PROJECT_NAMES
                        DB_CONN_FILENAME = 'DB_connection.txt',
                        DS_PARAMS_FILE_NAME = 'DS_PARAMS.txt', 
                        PIPELINE_START_LEVEL = 1)               #Starting level
```
PIPELINE_START_LEVEL; 
    1. Make_target_comp_tables  (when the first time)
    2. Table to rawSeq
    3. RawSeq to multihot
    4. Multihot to Dataset      (when you want to restore datasets)


## Release History

* 1.0.0
    * released

## Meta

Sanghyung Jin, MS(1) – jsh90612@gmail.com  
Yourim Lee, BS(1) - urimeeee.e.gmail.com  
Rae Woong Park, MD, PhD(1)(2) - rwpark99@gmail.com  

(1) Dept. of Biomedical Informatics, Ajou University School of Medicine, Suwon, South Korea  
(2) Dept. of Biomedical Sciences, Ajou University Graduate School of Medicine, Suwon, South Korea  
