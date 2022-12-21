from pathlib import Path
import logging
import os

from src.pipeline_factory import PipelineFactory
################
# Configurations#
################

INPUT_FOLDER = "./inputs"
OUTPUT_FOLDER = "./outputs"
LOGGING_INFO = "INFO"
PIPELINE_CHOICE = "pyspark"




def run_pipeline():
    logger = logging.getLogger('automated_pipeline')
    logger.setLevel(LOGGING_INFO)

    # Looping through year folder
    pathlist = Path(INPUT_FOLDER).glob("*/*")
    for path in pathlist:
        if path.is_dir():
            path_in_str = str(path)
            pipeline = PipelineFactory(PIPELINE_CHOICE,path_in_str,OUTPUT_FOLDER)
            pipeline.generate()


if __name__ == "__main__":
    run_pipeline()


