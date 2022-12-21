from abc import ABC, abstractmethod
from typing import Tuple, Dict, Any
import json
import numpy as np
from pathlib import Path

###########
#Functions#
###########

def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()

##########
# Classes#
##########
class BasePipeline(ABC):
    """
    BasePipeline Class that should be inherited by all implemented pipeline (implemented with different python libraries)
    It contains attributes and methods that are shared among all pipeline and define abstract methods

    Args:
        path_to_month_folder (str): path to month folder containing the following csv files- posts.csv and users.csv
        outputs_folder (str): folder name for output_folder

    Attributes:
        posts_filename (str): relative path to posts.csv based on what was passed in args (path_to_month_folder)
        users_filename (str): relative path to users.csv based on what was passed in args (path_to_month_folder)
        data_year (str): year information of data. It will be useful for preprocessing users.csv for January 2016 as it contains all users created until that point
        data_month (str): month information of data. It will be useful for preprocessing users.csv for January 2016 as it contains all users created until that point
        summary_table_filename (str): relative path to "summary_table.json" based on what was passed in args (output_folder)
        tag_analysis_filename (str):relative path to "tag_analysis.parquet" based on what was passed in args (output_folder)
    
    """    
    def __init__(self,  path_to_month_folder: str , outputs_folder: str) -> None:
        self.posts_filename = str(Path(path_to_month_folder)/"posts.csv")
        self.users_filename = str(Path(path_to_month_folder)/"users.csv")
        self.data_year, self.data_month = self._get_year_and_month_from_pathname(path_to_month_folder)

        (self.summary_table_filename, 
        self.tag_analysis_filename) = self._create_output_folder_structure(self.data_year, self.data_month, outputs_folder)

    
    def _get_year_and_month_from_pathname(
        self,
        pathname: str)-> Tuple [str,str]:
        """ Get year and month information from the pathname using Pathlib
        """
        year , month = Path(pathname).parts[-2:]
        return year, month

    def _create_output_folder_structure(
        self, 
        year: str,
        month: str ,
        output_folder: str
        )-> Tuple [str, str]:        
        """
        Helper function to create summary_table_filename and tag_analysis_filename

        Args:
            year (str): year number of the stackoverflow data
            month (str): month number of the stackoverflow data
            output_folder (str): folder name for output_folder

        Returns:
            Tuple[str, str]: summary_table_filename, tag_analysis_filename
        """        
        output_folder = Path(output_folder)/ year/ month
        Path(output_folder).mkdir(exist_ok=True, parents = True)

        class_name = self.__class__.__name__

        summary_table_filename = str(output_folder / f"{class_name}_summary_table.json")
        tag_analysis_filename = str(output_folder/ f"{class_name}_tag_analysis.parquet")
        
        return summary_table_filename, tag_analysis_filename
    
    @staticmethod
    def dict_to_json_file(dictionary: Dict [str, Any], out_filename: str):
        """
        Generate json file from a dictionary 

        Args:
            dictionary (Dict[str, Any]): 
            out_filename (str): filename of json file
        """    
        json_object = json.dumps(dictionary, default=np_encoder)
        with open(out_filename, "w") as out_file:
            out_file.write(json_object)

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
        in outputs folder as the following format
         - summary table (JSON format): {{output_folder}}/{{year}}/{{month}}/{{pipeline_name}}_summary_table.json
         - tag analysis table (parquet format): {{output_folder}}/{{year}}/{{month}}/{{pipeline_name}}_tag_analysis.parquet

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
            - year
        """        
        pass