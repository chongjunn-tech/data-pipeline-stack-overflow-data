from src.pyspark_pipeline import PysparkPipeline
from src.pandas_pipeline import PandasPipeline
from src.pandas_on_spark_pipeline import PandasOnSparkPipeline


############
# Functions#
############
def PipelineFactory(pipeline_choice: str, input_folder: str , output_folder:str):
    """
    Factory Method to generate pipeline as created by Data Engineer

    Args:
        pipeline_choice (str): Available choices for pipeline includes: "pandas", "pyspark"
        input_folder (str): path to input folder
        output_folder (str): path to output folder

    Returns:
        BasePipeline
    """    

    pipelines_available = {
        "pandas": PandasPipeline,
        "pyspark": PysparkPipeline,
        # "pandas_on_spark": PandasOnSparkPipeline, WIP as does not seems to have method to calculate most frequent element
    }
    if pipeline_choice not in pipeline_choice.keys():
        raise KeyError(f"pipeline choice not in {list(pipelines_available.keys())}")
    return pipelines_available.get(pipeline_choice)(
        input_folder = input_folder,
        output_folder = output_folder)
