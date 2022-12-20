
# Data Pipeline

This repo documents on how to create an automated pipeline to ingest two tables (StackOverflow posts and StackOverflow
users) to generate a summary table (JSON file) and a tag analysis table (parquet file)

## Installation
- dependencies of pyspark: To run pyspark based data pipeline (one of choice in `PipelineFactory`), you will need to install Java SE Development Kit. Please refer to JDK downloads [here](https://www.oracle.com/sg/java/technologies/downloads/)

- dependencies of app
```
conda create -n data-pipeline python=3.8 -y
conda activate data-pipeline
pip install -r requirements.txt

```
## Configurations
- configurations can be changed at config.env
    
## Usage/ Examples

Place input data in inputs folder

    .
    ├── ...
    ├── inputs                    
    │   └──  2016
    │       ├── 01       
    │       ├── 02        
    │       └── 03                
    └── ...




```
./run.sh
```

output summary table (JSON file) and a tag analysis table  will be generated in desired folder name


    .
    ├── ...
    ├── <chosen_folder_name>                    
    │   └──  2016
    │       ├── 01
    │       │   ├── tag_analysis.parquet       
    │       │   └── summary_table.json
    │       ├── 02
    │       │   ├── tag_analysis.parquet       
    │       │   └── summary_table.json        
    │       └── 03
    │       │   ├── tag_analysis.parquet       
    │       │   └── summary_table.json                
    └── ...