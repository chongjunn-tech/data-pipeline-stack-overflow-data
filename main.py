from pathlib import Path
import yaml

from src.pipeline_factory import PipelineFactory

######################
# Load configurations#
######################
FILE_PATH = "./config.yaml"

try:
    with open(FILE_PATH, "r") as f:
        CONFIG = yaml.safe_load(f)
except:
    raise FileNotFoundError(f"Configuration file {FILE_PATH} not found")



def run_pipeline(
    inputs_folder:str = "./inputs", 
    outputs_folder: str = "./outputs", 
    pipeline_choice: str = "pyspark"):
    """
    Loop through inputs_folder to get all path_to_month_folder(s) (e.g inputs/2016/01) in inputs folder
    and feed it to a pipeline based on choice by user (pipeline_choice)


    Run pipeline to process users.csv and posts.csv

    Args:
        inputs_folder (str): path to inputs folder
        outputs_folder (str): path to outputs folder
        pipeline_choice (str): Available choices for pipeline includes: "pandas", "pyspark"
    """    


    # Looping through inputs folder containing the following folder structure for data
    # inputs
    #  |- year
    #  |   |- month
    #  |       |- posts.csv
    #  |       |- users.csv
    generator = Path(inputs_folder).glob("*/*")
    pathlist = [path for path in generator if path.is_dir()]
    if not pathlist:
        raise FileNotFoundError(f"No data found in {inputs_folder}")
    else:
        for path_to_month_folder in pathlist:
            pipeline = PipelineFactory(pipeline_choice,path_to_month_folder,outputs_folder)
            pipeline.generate()


if __name__ == "__main__":

    run_pipeline(
        inputs_folder = CONFIG.get("INPUTS_FOLDER"),
        outputs_folder = CONFIG.get("OUTPUTS_FOLDER"),
        pipeline_choice= CONFIG.get("PIPELINE_CHOICE")
    )



