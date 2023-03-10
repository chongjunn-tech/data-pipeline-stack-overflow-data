#!/usr/bin/env python

import json
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any

# Custom
from src.base import BasePipeline

################
# Configurations#
################


##########
# Classes#
##########
class PandasPipeline(BasePipeline):
    def __init__(self, path_to_month_folder: str , outputs_folder: str) -> None:
        super().__init__(path_to_month_folder, outputs_folder)

    def _get_tag_hero(self, series: pd.Series):
        """Helper function for lambda function
        Its purpose is to get tag hero from panda Series "display_name".
        tag_hero refers to `display_name` of the user who created most posts with this tag in the
        current month.
        """
        # fill na value with empty string ""
        # na value occurs as some display_name fields are empty
        # If not, it will cause error when calculating mode
        series = series.fillna("")
        # calculating mode
        tag_hero = series.value_counts().index[0]

        return tag_hero

    def _load_data(self, posts_filename: str, users_filename: str):
        df_posts = pd.read_csv(posts_filename, low_memory=False)
        df_users = pd.read_csv(users_filename, low_memory=False)
        return df_posts, df_users

    def _preprocess_data(self, df_posts, df_users):
        # convert to datetime with pandas
        df_posts["creation_date"] = pd.to_datetime(df_posts["creation_date"])
        df_users["creation_date"] = pd.to_datetime(df_users["creation_date"])

        ## only for table for Jan 2016 as it contains all users created until that point
        ## while after that each months, contains only new users created in that month
        if self.data_year == "2016" and self.data_month == "1":
            selector = df_users["creation_date"].dt.year == ["2016"]
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
        average_comments_per_post = (
            df_posts["comment_count"].sum() / number_of_posts
        )

        # Number of new users created in the month.
        number_of_new_users = df_users["id"].count()

        # Total number of active tags in the month.
        tags_counts = (
            df_posts["tags"]
            .str.split("|")
            .map(lambda val: len(val) if isinstance(val, list) else 0)
        )
        total_active_tags = tags_counts.count()

        # Average number of posts per user.
        unique_users = df_posts["owner_user_id"].nunique()
        average_posts_per_user = number_of_posts / unique_users

        # avg_reputation_per_user
        average_reputation_per_user = df_users["reputation"].mean()

        # Histogram with the number of posts for each day
        histogram = df_posts["creation_date"].dt.day.value_counts().sort_index()
        histogram = histogram.to_dict()

        return {
            "number_of_posts": number_of_posts,
            "average_comments_per_post": average_comments_per_post,
            "number_of_new_users": number_of_new_users,
            "total_active_tags": total_active_tags,
            "average_posts_per_user": average_posts_per_user,
            "average_reputation_per_user": average_reputation_per_user,
            "histogram": histogram,
        }

    def compute_tag_analysis(
        self, df_posts: pd.DataFrame, df_users: pd.DataFrame
    ):
        """
        Computes the tag analysis table for a given month.

        Parameters:
            - df_posts: Pandas dataframe containing the posts data for the month.
            - df_users: Pandas dataframe containing the users data for the month.

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
        df = df_posts[["tags", "owner_user_id", "score"]]

        # Extract the month and year from the creation_date column
        # and assign to df
        df_posts_creation_date = df_posts.loc[:,"creation_date"]
        df_posts_creation_year =  df_posts_creation_date.dt.year
        df_posts_creation_month = df_posts_creation_date.dt.month
        df = df.assign(year = df_posts_creation_year)
        df = df.assign(month = df_posts_creation_month)
        

        # Split the tags field into a list of tags.
        df = df.assign(tags=lambda x: x.tags.str.split("|"))

        # Unpack the list of tags into separate rows.
        df = df.explode("tags")

        # Rename the tags column to tag.
        df = df.rename(columns={"tags": "tag"})

        # Join the df dataframe with the df_users dataframe on the owner_user_id field.
        df_tag = df.merge(
            df_users[["id", "reputation", "display_name"]],
            left_on="owner_user_id",
            right_on="id",
        )

        # Group the data by tag and month, and compute the following statistics:
        # - Number of posts that contain this tag in the month.
        # - Average reputation of the users who create posts with this tag.
        # - Average score of the posts that contain this tag.
        indexes = ["tag", "month", "year"]
        tag_analysis = df_tag.groupby(indexes).agg(
            {
                "owner_user_id": "count",
                "reputation": "mean",
                "score": "mean",
                "display_name": lambda series: self._get_tag_hero(series),
            }
        )

        column_to_analysis_name = {
            "owner_user_id": "posts",
            "reputation": "avg_reputation",
            "score": "avg_score",
            "display_name": "tag_hero",
        }
        tag_analysis = tag_analysis.rename(columns=column_to_analysis_name)
        # Return the resulting dataframe.
        return tag_analysis

    def generate(self):

        (df_posts, df_users) = self._load_data(
            self.posts_filename, self.users_filename
        )

        (df_posts, df_users) = self._preprocess_data(df_posts, df_users)

        ### Summary table
        summary_table = self.compute_summary(df_posts, df_users)
        self.dict_to_json_file(summary_table, self.summary_table_filename)
        print(
            f"Summary table (JSON file) - {self.summary_table_filename} created"
        )

        ### tag analysis table
        tag_analysis_table = self.compute_tag_analysis(df_posts, df_users)
        tag_analysis_table.to_parquet(
            self.tag_analysis_filename, partition_cols=["year", "month"]
        )
        print(
            f"Tag analysis table (Parquet file) - {self.tag_analysis_filename} created"
        )
