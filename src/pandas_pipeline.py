#!/usr/bin/env python

import json
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any
# Custom
from src.base import BasePipeline

################
#Configurations#
################



##########
# Classes#
##########
class PandasPipeline(BasePipeline):
    
    def __init__(self, input_folder: str, output_folder: str) -> None:
        super().__init__(input_folder, output_folder)

    def _get_one_tag_hero(self, values):
        """ Get one tag hero only when series.mode return a list of values (same mode)
        """        
        if isinstance(values,np.ndarray):
            if len(values)>=1:
                return values[0]
            else:
                return ''
        else:
            return values

    def _load_data(self, posts_filename:str, users_filename:str):
        df_posts = pd.read_csv(posts_filename, low_memory=False)
        df_users = pd.read_csv(users_filename, low_memory=False)
        return df_posts, df_users

    def _preprocess_data(self, df_posts, df_users):
        # convert to datetime with pandas
        df_posts['creation_date'] = pd.to_datetime(df_posts['creation_date'])
        df_users['creation_date'] = pd.to_datetime(df_users['creation_date'])
        
        ## only for table for Jan 2016 as it contains all users created until that point
        ## while after that each months, contains only new users created in that month
        if df_users["creation_date"].dt.year.nunique() > 1:
            latest_year = df_users["creation_date"].dt.year.max()
            selector = df_users["creation_date"].dt.year == latest_year
            df_users = df_users.loc[selector]
        
        return df_posts, df_users



    def compute_summary(self, df_posts: pd.DataFrame, df_users: pd.DataFrame):
        """
        Computes the summary table for a given month.
        
        Parameters:
            - df_posts: Pandas dataframe containing the posts data for the month.
            - df_users: Pandas dataframe containing the users data for the month.
        
        Returns:
            A dictionary containing the following information:
            - number_of_posts: Number of posts in the month.
            - average_comments_per_post: Average number of comments per post.
            - number_of_new_users: Number of new users created in the month.
            - total_active_tags: Total number of active tags in the month.
            - average_posts_per_user: Average number of posts per user.
            - average_reputation_per_user: Average reputation per user.
            - histogram: A histogram with the number of posts for each day.
        """

        # Number of posts in the month
        number_of_posts = df_posts.shape[0]

        # Average number of comments per post.
        average_comments_per_post = sum(df_posts["comment_count"])/ number_of_posts

        # Number of new users created in the month.
        number_of_new_users = df_users["id"].count()

        # Total number of active tags in the month.
        tags_counts = df_posts["tags"].str.split("|").map(
            lambda val: len(val) if isinstance(val,list) else 0)
        total_active_tags = tags_counts.count()

        # Average number of posts per user.
        unique_users = df_posts["owner_user_id"].nunique()
        average_posts_per_user = number_of_posts/ unique_users

        # avg_reputation_per_user
        average_reputation_per_user = df_users["reputation"].mean()

        # Histogram with the number of posts for each day
        histogram = df_posts['creation_date'].dt.day.value_counts().sort_index()
        histogram = histogram.to_dict()

        return {
        'number_of_posts': number_of_posts,
        'average_comments_per_post': average_comments_per_post,
        'number_of_new_users': number_of_new_users,
        'total_active_tags': total_active_tags,
        'average_posts_per_user': average_posts_per_user,
        'average_reputation_per_user': average_reputation_per_user,
        'histogram': histogram
        }

    def compute_tag_analysis(self, df_posts: pd.DataFrame, df_users: pd.DataFrame):
        """
        Computes the tag analysis table for a given month.
        
        Parameters:
            - df_posts: Pandas dataframe containing the posts data for the month.
            - df_users: Pandas dataframe containing the users data for all months.
        
        Returns:
            A Pandas dataframe with the following columns:
            - tag: Name of the tag.
            - posts: Number of posts that contain this tag in the month.
            - avg_reputation: Average reputation of the users who create posts with this tag.
            - avg_score: Average score of the posts that contain this tag.
            - tag_hero: display_name of the user(s) who created most posts with this tag in the month.
            - month
            - year
        """
        # Create a new dataframe with the following columns:
        # - tag: Name of the tag.
        # - owner_user_id: Id of the user who created the post.
        # - score: Score of the post.
        df = df_posts[['tags', 'owner_user_id', 'score']]

        # Extract the month and year from the creation_date column.
        df.loc[:,'month'] = df_posts['creation_date'].dt.month
        df.loc[:,'year'] = df_posts['creation_date'].dt.year

        # Split the tags field into a list of tags.
        df.assign(tags = lambda x : x.tags.str.split("|"))

        # Unpack the list of tags into separate rows.
        df = df.explode('tags')
    
        # Rename the tags column to tag.
        df = df.rename(columns={'tags': 'tag'})
        
        # Join the df dataframe with the df_users dataframe on the owner_user_id field.
        df_tag = df.merge(
            df_users[['id', 'reputation', 'display_name']], 
            left_on='owner_user_id', 
            right_on='id'
            )

        # Group the data by tag and month, and compute the following statistics:
        # - Number of posts that contain this tag in the month.
        # - Average reputation of the users who create posts with this tag.
        # - Average score of the posts that contain this tag.
        tag_analysis = df_tag.groupby(['tag', 'month', 'year']) \
            .agg({'owner_user_id': 'count',
                    'reputation': 'mean',
                    'score': 'mean',
                    "display_name": pd.Series.mode}) \
            .rename(columns={'owner_user_id': 'posts',
                                'reputation': 'avg_reputation',
                                'score': 'avg_score',
                                "display_name": "tag_hero"})

        tag_analysis["tag_hero"] = tag_analysis["tag_hero"].map(lambda val: self._get_one_tag_hero(val))
        # Return the resulting dataframe.
        return tag_analysis

    def generate(self):
    
        (df_posts, df_users) = self._load_data(self.posts_filename,self.users_filename)

        (df_posts, df_users) = self._preprocess_data(df_posts, df_users)

        ### Summary table
        summary_table = self.compute_summary(df_posts, df_users)
        PandasPipeline.dict_to_json_file(summary_table,self.summary_table_filename)
        logging.info(f"Summary table (JSON file) for {self.input_folder} created : {self.summary_table_filename}")

        ### tag analysis table
        tag_analysis_table = self.compute_tag_analysis(df_posts, df_users)
        tag_analysis_table.to_parquet(self.tag_analysis_filename, partition_cols=['year', 'month'])
        logging.info(f"Tag analysis table (Parquet file) for {self.input_folder} created : {self.tag_analysis_filename}")


