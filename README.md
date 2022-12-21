
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
configurations can be changed at config.yaml
```
INPUTS_FOLDER: "./inputs" # folder where you place the data
OUTPUTS_FOLDER: "./outputs" # free-text
PIPELINE_CHOICE: "pyspark" #Available choices for pipeline includes: "pandas", "pyspark"
```

    
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
python main.py
```

output summary table (JSON file) and a tag analysis table  will be generated in desired folder name (default: "./outputs" )


    .
    ├── ...
    ├── outputs                   
    │   └──  2016
    │       ├── 01
    │       │   ├── {{pipeline_name}}_tag_analysis.parquet       
    │       │   └── {{pipeline_name}}_summary_table.json
    │       ├── 02
    │       │   ├── {{pipeline_name}}_tag_analysis.parquet       
    │       │   └── {{pipeline_name}}_summary_table.json        
    │       └── 03
    │       │   ├── {{pipeline_name}}_tag_analysis.parquet       
    │       │   └── {{pipeline_name}}_summary_table.json                
    └── ...