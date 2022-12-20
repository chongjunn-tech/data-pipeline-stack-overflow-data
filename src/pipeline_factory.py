from src.pyspark_pipeline import PysparkPipeline
from src.pandas_pipeline import PandasPipeline
from src.pandas_on_spark_pipeline import PandasOnSparkPipeline


############
# Functions#
############
def PipelineFactory(pipeline_choice: str, input_folder: str, output_folder:str):
    """Factory Method to generate pipeline
    Available choices for pipeline includes:
        - "pandas": PandasPipeline
        - "pyspark" : PysparkPipeline
    """
    pipelines_available = {
        "pandas": PandasPipeline,
        "pyspark": PysparkPipeline,
        # "pandas_on_spark": PandasOnSparkPipeline, WIP as does not seems to have method to calculate most frequent element
    }
    return pipelines_available.get(pipeline_choice)(
        input_folder = input_folder,
        output_folder = output_folder)
