from src.base import BasePipeline
from src.pyspark_pipeline import PysparkPipeline
from src.pandas_pipeline import PandasPipeline
from src.pandas_on_spark_pipeline import PandasOnSparkPipeline


############
# Functions#
############
def PipelineFactory(pipeline_choice: str, path_to_month_folder: str , outputs_folder:str) -> BasePipeline:
    """
    Factory Method to generate different pipeline class based on pipeline_choice
    Different pipeline class make use of different python libraries to compute data

    Args:
        pipeline_choice (str): Available choices for pipeline includes: "pandas", "pyspark"
        path_to_month_folder (str): path to month folder containing the following csv files- posts.csv and users.csv
        outputs_folder (str): path to output folder

    Returns:
        BasePipeline
    """    

    pipelines_available = {
        "pandas": PandasPipeline,
        "pyspark": PysparkPipeline,
        # "pandas_on_spark": PandasOnSparkPipeline, WIP as does not seems to have method to calculate most frequent element
    }
    if pipeline_choice not in pipelines_available.keys():
        raise KeyError(f"pipeline choice not in {list(pipelines_available.keys())}")
    return pipelines_available.get(pipeline_choice)(
        path_to_month_folder = path_to_month_folder,
        outputs_folder = outputs_folder)
