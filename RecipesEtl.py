

import argparse
import os
import sys
import isodate
import pyspark.sql.functions as sql_func
from dependencies.spark_utils import start_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,BooleanType,ArrayType,StructField,StructType,StringType



class RecipesETL:
    """
   This Class provides ETL implementation using pyspark with features to perform the transformation as per the business logic 

   Added Functionality to filter out the error records created during the transformation 

    """
    # Holds transformed dataframe for further process
    transform_df=None
   
    def filter_error_records(self, dataframe,column_name):

        """
        Filters the Error records during the transformation for  the specified input column
        :param: Transformed Dataframe
        :param: Error for column
        :return: dataframe(column,transformed_column)
        """

        return dataframe.where(sql_func.col(column_name+"Processed")["transform_status"] =="ERROR").select(sql_func.col("name"),sql_func.col(column_name),sql_func.col(column_name+"Processed"))

    def apply_fun_to_array(self, func):
        """
        Creates a UDF which applies the filter function and checks for all elemenst in collect
        :param func: function
        :return: udf 
        """
        return udf(lambda xs: any(func(x) for x in xs), BooleanType())

    def iso_time_to_min(self, iso_str):

        """
        Converts the duration time in ISO time formate to minutes
        :param iso_str: ISO time String
        :return: Tuple(status, duration minutes)
        """
        try:
            if(not (iso_str.strip())):
                return ("ERROR",0)
            duration = (isodate.parse_duration(iso_str).seconds // 60)
        except Exception:
            return ("ERROR",0)
        return ("SUCCESS",duration)

    def load_transform(self,spark, input_path):

        """
        This function loads and transforms the data from the given input path
        :param spark: Spark Session
        :param input_path: Source data input path 
        :return: Trasformed dataframe 
        """

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
        ).cache()
        return transform_df

    def difficulty_metrics(self,data_df=None):
        """
        Provides metrics based on the difficulty level provided as per the business logic
        :param data_df: dataframe (default is transform_df)
        :return : metrics_df
        """
        if not(data_df):
            data_df=self.transform_df
        transform_df = data_df.withColumn(
            "difficulty",
            sql_func.when(sql_func.col("total_cook_time")<30, "easy").when((sql_func.col("total_cook_time")<=30) | (sql_func.col("total_cook_time")<=60 ), "medium")
            .when(sql_func.col("total_cook_time")>60, "hard"))
        group_df=transform_df.groupBy("difficulty").agg({'total_cook_time':'avg'}).select(sql_func.col("difficulty"),sql_func.round(sql_func.col("avg(total_cook_time)"),2).alias("avg_total_cooking_time"))
        return group_df.coalesce(1)

    def filter_data(self,filter_str):
        """
        Filters rows with secified column value
        :params: filter_value string
        :return: filterd dataframe 
        """
        filter_value=filter_str.lower()
        return self.transform_df.filter(
            self.apply_fun_to_array(
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
        parser.add_argument('-c', '--config_path', default="/configs/filter_config.json",     help='Config File Path')
        args = parser.parse_args()

    except Exception:
        parser.print_help()
        sys.exit(1)
    
  
    recipes_etl = RecipesETL()
    # Start spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='Recipes_ETL',
        files=[args.config_path])
    # Read and pre-process rows to ensure further optimal structure and performance for further processing
    recipes_etl.transform_df = recipes_etl.load_transform(spark,args.input_path)
    log.info('Tranformation completed')

    #Log Bad Records or records with Null Values
    log.error(recipes_etl.filter_error_records(recipes_etl.transform_df,"cookTime")._jdf.showString(100, int(False), False))
    
    #Extract only recipes that have beef as one of the ingredients
    
    filter_value=config['filter_ingredients']

    ingredientsFiltered=recipes_etl.filter_data(filter_value)
    # Prints the Filtered records 
    ingredientsFiltered.write.mode('overwrite').save("./output/ingredientsFiltered", format='parquet')
   
    # Difficulty metrics based on the provided formulae for entire dataset
    metrics=recipes_etl.difficulty_metrics()

    metrics.write.mode('overwrite').save(args.output_path, format='csv', header=True)
    # Difficulty metrics based on the provided formulae for filtered ingredient
    filtered_metrics=recipes_etl.difficulty_metrics(ingredientsFiltered)
    filtered_metrics.write.mode('overwrite').save("./output/reportFiltered.csv", format='csv', header=True)
