"""
Script documentation
"""

import argparse
import os
import sys

import isodate
import pyspark.sql.functions as sql_func
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,BooleanType,ArrayType,StructField,StructType,StringType
from collections import defaultdict

def iso_time_to_min( iso_str):
    try:
        if(not (iso_str.strip())):
            return ("ERROR",0)
        duration = (isodate.parse_duration(iso_str).seconds % 3600) / 60
    except:
        return ("ERROR",0)
    return ("SUCCESS",duration)


class RecipesAnalytics:
    """
    Class doumentation
    """
    transform_df=None
   
    def __init__(self, input_path,output_path,filter_value):
        self.input_path = input_path
        self.output_path=output_path
        self.filter_value=filter_value


    def filter_spark_data_frame(self, dataframe, column_name='age',value=20):
        return dataframe.where(sql_func.col(column_name) > value)

    def filter_error_records(self, dataframe,column_name):

        return dataframe.where(sql_func.col(column_name+"Processed")["transform_status"] =="ERROR").select(sql_func.col("name"),sql_func.col(column_name),sql_func.col(column_name+"Processed"))

    def exists_in_array(self, func):
        return udf(lambda xs: any(func(x) for x in xs), BooleanType())

    def iso_time_to_min(self, iso_str):
        try:
            if(not (iso_str.strip())):
                return ("ERROR",0)
            duration = (isodate.parse_duration(iso_str).seconds // 60)
        except Exception:
            return ("ERROR",0)
        return ("SUCCESS",duration)

    def pre_process(self,spark, input_path):
        source_df = spark.read.json(input_path)
        schema = StructType([StructField("transform_status", StringType(), False),StructField("value", IntegerType(), False)])

        iso_time_to_min_udf = udf(self.iso_time_to_min, schema)
        transform_df = source_df.withColumn(
            "ingredientsProcessed", sql_func.split(sql_func.col('ingredients'), "\n")
        ).withColumn(
            "cookTimeProcessed", iso_time_to_min_udf(source_df['cookTime'])
        ).withColumn(
            "prepTimeProcessed", iso_time_to_min_udf(source_df['prepTime'])
        ).withColumn(
            "total_cook_time",
            iso_time_to_min_udf(source_df['cookTime'])["value"] + iso_time_to_min_udf(source_df['prepTime'])["value"]
        )
        #self.transform_columns["ingredients"]="ingredientsProcessed"
        return transform_df

    def difficulty_metrics(self,data_df=None):
        if not(data_df):
            data_df=self.transform_df
        transform_df = data_df.withColumn(
            "difficulty",
            sql_func.when(sql_func.col("total_cook_time")<30, "easy").when((sql_func.col("total_cook_time")<=30) | (sql_func.col("total_cook_time")<=60 ), "medium")
            .when(sql_func.col("total_cook_time")>60, "hard"))
        group_df=transform_df.groupBy("difficulty").agg({'total_cook_time':'avg'}).select(sql_func.col("difficulty"),sql_func.round(sql_func.col("avg(total_cook_time)"),2).alias("avg_total_cooking_time"))
        return group_df.coalesce(1)

    def filter_data(self):
        filter_value=self.filter_value.lower()
        return self.transform_df.filter(
            self.exists_in_array(
                lambda x: filter_value in x.lower())("ingredientsProcessed")
        )

if __name__ == '__main__':
    """
    ETL for Analysis of Receipe Data

    This Function checks command line arguments and if the arguments are not passed it will takes the default values
    """
    try:

        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
                '-i', '--input_path', default="./input/recipes.json",     help='Input File Path')
        parser.add_argument(
                '-o', '--output_path', default="./output/report.csv", help='Output File Path')
        # parser.add_argument('-fc', '--filterColumn', default="ingredients",     help='Filter Column')
        parser.add_argument('-fv', '--filter_value', default="beef",     help='Filter Values')
        args = parser.parse_args()

    except Exception:
        parser.print_help()
        sys.exit(1)
    
    # Read and pre-process rows to ensure further optimal structure and performance for further processing
    recipes_analytics = RecipesAnalytics(args.input_path,args.output_path,args.filter_value)
    spark = SparkSession.builder.master('local').getOrCreate()
    recipes_analytics.transform_df = recipes_analytics.pre_process(spark,args.input_path)
    #Bad Records or records with Null Values
    recipes_analytics.filter_error_records(recipes_analytics.transform_df,"cookTime").show()
    #Extract only recipes that have beef as one of the ingredients
    ingredientsFiltered=recipes_analytics.filter_data()
    # Prints the Filtered records count
    print("FILTERED COUNT >>>",ingredientsFiltered.count())
    # Difficulty metrics based on the provided formulae for entire dataset
    metrics=recipes_analytics.difficulty_metrics()
    metrics.show()
    metrics.write.mode('overwrite').save(args.output_path, format='csv', header=True)
    # Difficulty metrics based on the provided formulae for filtered ingredient
    filtered_metrics=recipes_analytics.difficulty_metrics(ingredientsFiltered)
    filtered_metrics.write.mode('overwrite').save("./output/reportFiltered.csv", format='csv', header=True)
