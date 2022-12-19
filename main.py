
from pathlib import Path
import logging
from src.pandas_pipeline import PandasPipeline


################
#Configurations#
################
DATA_YEAR_FOLDER = "./inputs/2016"
OUTPUT_FOLDER = "./outputs"
LOGGING_INFO = logging.INFO



if __name__ =="__main__":

    logging.basicConfig(level=LOGGING_INFO)

    # Looping through year folder
    pathlist = Path(DATA_YEAR_FOLDER).glob('*/')
    for path in pathlist:
        if path.is_dir():
            path_in_str = str(path)

            pandas_pipeline = PandasPipeline(
                input_folder= path_in_str,
                output_folder = OUTPUT_FOLDER
            )
            pandas_pipeline.generate()

