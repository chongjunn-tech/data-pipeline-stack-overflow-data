from pathlib import Path
import logging
from src.pandas_pipeline import PandasPipeline
from src.pandas_on_spark_pipeline import PandasOnSparkPipeline
import os

################
# Configurations#
################
DATA_YEAR_FOLDER = "./inputs/2016"
OUTPUT_FOLDER = "./outputs"
LOGGING_INFO = logging.INFO
PIPELINE_CHOICE = "pandas"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


############
# Functions#
############
def PipelineFactory(pipeline_choice: str, input_folder: str, output_folder:str):
    """Factory Method to generate pipeline
    Available choices for pipeline includes:
        - "pandas": PandasPipeline
        - "pandas_on_spark": PandasOnSparkPipeline
    """
    pipelines_available = {
        "pandas": PandasPipeline,
        "pandas_on_spark": PandasOnSparkPipeline,
    }
    return pipelines_available.get(pipeline_choice)(
        input_folder = input_folder,
        output_folder = output_folder)

if __name__ == "__main__":

    logging.basicConfig(level=LOGGING_INFO)

    # Looping through year folder
    pathlist = Path(DATA_YEAR_FOLDER).glob("*/")
    for path in pathlist:
        if path.is_dir():
            path_in_str = str(path)

            chosen_pipeline = PandasPipeline(
                input_folder=path_in_str, output_folder=OUTPUT_FOLDER
            )
            # chosen_pipeline = PandasOnSparkPipeline(
            #     input_folder=path_in_str, output_folder=OUTPUT_FOLDER
            # )
            pipeline = PipelineFactory(PIPELINE_CHOICE,path_in_str,OUTPUT_FOLDER)
            pipeline.generate()
