from pathlib import Path
import logging
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
    logging_info: str, 
    inputs_folder:str, 
    outputs_folder:str, 
    pipeline_choice: str):

    logger = logging.getLogger('automated_pipeline')
    logger.setLevel(logging_info)

    # Looping through inputs folder containing folder structure
    # inputs
    #  |- year
    #      |- month
    pathlist = Path(inputs_folder).glob("*/*")
    for path_to_month_folder in pathlist:
        if path_to_month_folder.is_dir():
            pipeline = PipelineFactory(pipeline_choice,path_to_month_folder,outputs_folder)
            pipeline.generate()


if __name__ == "__main__":
    run_pipeline(
        logging_info = CONFIG.get("LOGGING_INFO"),
        inputs_folder = CONFIG.get("INPUTS_FOLDER"),
        outputs_folder = CONFIG.get("OUTPUTS_FOLDER"),
        pipeline_choice= CONFIG.get("PIPELINE_CHOICE")
    )



