from pathlib import Path
import logging
import os

from src.pipeline_factory import PipelineFactory
################
# Configurations#
################

# DATA_YEAR_FOLDER = "./inputs"
# OUTPUT_FOLDER = "./outputs"
# LOGGING_INFO = "INFO"
# PIPELINE_CHOICE = "pyspark"

INPUT_FOLDER = os.environ.get("INPUT_FOLDER")
OUTPUT_FOLDER = os.environ.get("OUTPUT_FOLDER")
LOGGING_INFO = os.environ.get("LOGGING_INFO")
PIPELINE_CHOICE = os.environ.get("PIPELINE_CHOICE")


if __name__ == "__main__":

    logger = logging.getLogger('automated_pipeline')
    logger.setLevel(LOGGING_INFO)

    # Looping through year folder
    pathlist = Path(INPUT_FOLDER).glob("*/*")
    for path in pathlist:
        if path.is_dir():
            path_in_str = str(path)
            pipeline = PipelineFactory(PIPELINE_CHOICE,path_in_str,OUTPUT_FOLDER)
            pipeline.generate()
