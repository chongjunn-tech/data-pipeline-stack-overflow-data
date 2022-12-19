
# Data Pipeline

This repo documents on how to create an automated pipeline to ingest two tables (StackOverflow posts and StackOverflow
users) to generate a summary table (JSON file) and a tag analysis table (parquet file)

Summary table, a JSON file with the following info:
- Number of posts in that month
- Average number of comments per post
- Number of new users created in that month
- Total number of active tags in that month
- Average number of posts per user
- Average reputation per user
- A histogram with the number of posts for each day

Tag analysis table, a parquet file partitioned by year and month with the following
columns:
- tag: Name of the tag
- posts: Number of posts that contain this tag on the current month
- avg_reputation: Average reputation of the users who create posts with this tag
- avg_score: Average score of the posts that contain this tag
- tag_hero: `display_name` of the user who created most posts with this tag in the
current month
- month
- year
## Installation

conda
```
conda create -n data-pipeline python=3.8 -y
conda activate data-pipeline
pip install -r requirements.txt

```

    
## Usage/Examples

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