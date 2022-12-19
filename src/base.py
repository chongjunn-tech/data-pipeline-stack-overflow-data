from abc import ABC, abstractmethod
from typing import Tuple
from pathlib import Path

class BasePipeline(ABC):
    """
    BasePipeline Class that should be inherited by all implemented pipeline (implemented with different python libraries)
    It contains attributes and methods that are shared among all pipeline and define abstract methods

    Attributes:
        input_folder (str): folder name for input_folder
        posts_filename (str): relative path to posts.csv
        users_filename (str): relative path to users.csv
        summary_table_filename (str): relative path to "summary_table.json"
        tag_analysis_filename (str):relative path to "tag_analysis.parquet"


    Args:
        input_folder (str): folder name for input_folder
        output_folder (str): folder name for output_folder
    
    
    """    
    def __init__(self, input_folder: str, output_folder: str) -> None:
        self.input_folder = input_folder
        self.posts_filename = Path(input_folder)/"posts.csv"
        self.users_filename = Path(input_folder)/"users.csv"
        (self.summary_table_filename, 
        self.tag_analysis_filename) = self._create_output_folder_structure(input_folder,output_folder)

    
    def _create_output_folder_structure(
        self, 
        input_folder: str, 
        output_folder: str
        )-> Tuple [ str, str]:        
        """
        Helper function to create summary_table_filename and tag_analysis_filename

        Args:
            input_folder (str): folder name for input_folder
            output_folder (str): folder name for output_folder

        Returns:
            Tuple[str, str]: summary_table_filename, tag_analysis_filename
        """        
        year , month = Path(input_folder).parts[-2:]
        output_folder = Path(output_folder)/ year/ month
        Path(output_folder).mkdir(exist_ok=True, parents = True)

        summary_table_filename = output_folder / "summary_table.json"
        tag_analysis_filename = output_folder/ "tag_analysis.parquet"
        
        return summary_table_filename, tag_analysis_filename

    @abstractmethod
    def _preprocess_data(self):
        """
        Preprocessing data implementation will require the following steps for the dataset
        - converting creation_date to relevant datetime format
        - filtering users in users.csv for Jan 2016 to only include Jan 2016 as The table for
          January 2016 contains all users created until that point, and after that each month
          contains only new users created in that month
        """        
        pass

    @abstractmethod
    def _load_data(self):
        """
        Loading data from file into memory in respective Python libraries' implementation
        """        
        pass

    @abstractmethod
    def compute_summary(self):
        """
        Computes the summary table for a given month. 
        """        
        pass
     
    @abstractmethod
    def compute_tag_analysis(self):
        """
        Computes the tag analysis table for a given month.
        """        
        pass

    @abstractmethod
    def generate(self):
        """
        Generates summary table (json file) and tag analysis table (parquet file partitioned by year and month)

        Summary table, a JSON file with the following info:
            -  Number of posts in that month
            -  Average number of comments per post
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
            - tag_hero: `display_name` of the user who created most posts with this tag in the current month
            - month
            -  year
        """        
        pass