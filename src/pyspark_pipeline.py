from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType,FloatType, DateType
from pyspark.sql.window import Window

import json
import numpy as np
import logging
from typing import Dict, Any
import os

# Custom
from src.base import BasePipeline, np

#################
# Configurations#
#################

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

POSTS_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("answer_count", IntegerType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("owner_user_id", LongType(), True),
    StructField("score", IntegerType(), True),
    StructField("tags", StringType(), True),
    StructField("view_count", IntegerType(), True)
])

USERS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("display_name", StringType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("reputation", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("up_votes", IntegerType(), True),
    StructField("down_votes", IntegerType(), True)
])


# Create a SparkSession
spark = SparkSession.builder.appName("StackOverflow Analysis").getOrCreate()

##########
# Classes#
##########
class PysparkPipeline(BasePipeline):
    def __init__(self, path_to_month_folder: str , outputs_folder: str) -> None:
        super().__init__(path_to_month_folder, outputs_folder)

    def _preprocess_data(self, df_posts, df_users):

        ## only for table for Jan 2016 as it contains all users created until that point
        ## while after that each months, contains only new users created in that month
        if self.data_year == "2016" and self.data_month == "1":
            selector = df_users["creation_date"].dt.year == ["2016"]
            df_users = df_users.loc[selector]

        return df_posts, df_users

    def _load_data(self, posts_filename: str, users_filename: str):
        # Read the Posts and Users data into Spark DataFrames
        df_post = spark.read.csv(posts_filename, header=True, schema=POSTS_SCHEMA)
        df_users = spark.read.csv(users_filename, header=True, schema=USERS_SCHEMA)
        return df_post, df_users
    
    def compute_summary(self, df_posts, df_users):
        
        # Number of posts in the month
        number_of_posts = df_posts.count()

        # Average number of comments per post.
        average_comments_per_post = df_posts.agg({"comment_count": "mean"}).collect()[0][0]
        
        #avg_reputation_per_user
        #  Number of new users created in the month.
        number_of_new_users, average_reputation_per_user= df_users.agg(
            {
                "id": "count",
                "reputation": "mean"
            }).collect()[0]

        # Total number of active tags in the month.
        tags_counts = df_posts.withColumn("tags_count", F.size(F.split(df_posts["tags"],"|")))
        total_active_tags = tags_counts.count()

        # Average number of posts per user.
        unique_users = df_posts.select(F.countDistinct("owner_user_id")).collect()[0][0]
        average_posts_per_user = number_of_posts / unique_users

        # Histogram with the number of posts for each day
        histogram = df_posts.groupBy(F.dayofmonth("creation_date").alias("day")) \
                                    .count().orderBy("day").collect()
        return {
            "number_of_posts": number_of_posts,
            "average_comments_per_post": average_comments_per_post,
            "number_of_new_users": number_of_new_users,
            "total_active_tags": total_active_tags,
            "average_posts_per_user": average_posts_per_user,
            "average_reputation_per_user": average_reputation_per_user,
            "histogram": histogram,
        }
    
    def compute_tag_analysis(self,df_posts, df_users):


        # Create a new column for the year
        df_posts = df_posts.withColumn("year", F.year(df_posts["creation_date"]))

        # Create a new column for the month
        df_posts = df_posts.withColumn("month", F.month(df_posts["creation_date"]))
        

        # Select the desired columns and create a new DataFrame
        df = df_posts.select(["tags", "owner_user_id", "score","year","month"])

        # Split the tags field into a list of tags. -F.split
        # Unpack the list of tags into separate rows -F.explode
        df = df.withColumn("tag", F.explode(F.split(df["tags"],pattern = '\\|')))

        # Join the df dataframe with the df_users dataframe on the owner_user_id field.

        df_tag = df.join(
            other = df_users.select(["id", "reputation", "display_name"]),
            on = df.owner_user_id == df_users.id,
            how = "left"
        )
        
        indexes = ["tag", "month", "year"]

        tag_analysis = df_tag.groupby(indexes).agg(
            F.count("owner_user_id").alias("posts"),
            F.avg("reputation").alias("avg_reputation"),
            F.avg("score").alias("avg_score"),
        )

        tag_hero_aggregate = (df_tag.groupby(indexes+["display_name"])
        .count()
        .withColumn("count_display_name", F.array("count", "display_name"))
        .groupby(indexes)
        .agg(F.max("count_display_name").getItem(1).alias("tag_hero"))) 

        tag_analysis = tag_analysis.join(
            other = tag_hero_aggregate,
            on = (tag_analysis.tag == tag_hero_aggregate.tag) & \
                  (tag_analysis.month ==tag_hero_aggregate.month) & \
                  (tag_analysis.year ==tag_hero_aggregate.year)  ,
            how = "left"
        )
        
        # drop additional column
        tag_analysis = tag_analysis.drop(*indexes)

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
            f"Summary table (JSON file)- {self.summary_table_filename} created"
        )

        ### tag analysis table
        tag_analysis_table = self.compute_tag_analysis(df_posts, df_users)
        tag_analysis_table.write.mode("overwrite").parquet(self.tag_analysis_filename)

        print(
            f"Tag analysis table (Parquet file) - {self.tag_analysis_filename} created"
        )
