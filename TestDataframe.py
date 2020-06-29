
from pyspark.sql import SparkSession
from RecipesEtl import RecipesETL
import unittest
import pandas as pd

class TestDataframe(unittest.TestCase):
    """
    Test cases to cover scenarios of failure during the ETL transformation and metrics creation
    """
    @classmethod
    def setUp(self):
        self.spark = SparkSession.builder.master('local').getOrCreate()
        self.spark.sparkContext.addFile("./RecipesEtl.py")
        self.recipes_analytics = RecipesETL()
        self.recipes_analytics.transform_df = self.recipes_analytics.load_transform(self.spark,"./input/testInput.json")

    def test_transform(self):

        def get_sorted_data_frame(data_frame, columns_list):
            return data_frame.sort_values(columns_list).reset_index(drop=True)


        transform_df=self.recipes_analytics.transform_df.select("cookTime","prepTime","total_cook_time")
        transform_df.show()
        expected_output = self.spark.createDataFrame(
            [('PT','PT15M',15),
            ('bla bla','PT6M',6),
            ('PT15M','PT15M',30),
            ('PT1H15M','PT10M',85)],
            ['cookTime','prepTime','total_cook_time'],
        )

        real_output = get_sorted_data_frame(
            transform_df.toPandas(),
            ['cookTime','prepTime','total_cook_time'],
        )
        expected_output = get_sorted_data_frame(
            expected_output.toPandas(),
            ['cookTime','prepTime','total_cook_time'],
        )
        pd.testing.assert_frame_equal(expected_output, real_output,     check_like=True,check_dtype=False)
    

    def test_metrics(self):

        def get_sorted_data_frame(data_frame, columns_list):
            return data_frame.sort_values(columns_list).reset_index(drop=True)

        metrics_df=self.recipes_analytics.difficulty_metrics()
        metrics_df.show()
        expected_output = self.spark.createDataFrame(
            [('medium', 30.0),
              ('hard', 85.0),
              ('easy', 10.5)],
            ['difficulty', 'avg_total_cooking_time'],
        )

        real_output = get_sorted_data_frame(
            metrics_df.toPandas(),
            ['difficulty', 'avg_total_cooking_time'],
        )
        expected_output = get_sorted_data_frame(
            expected_output.toPandas(),
            ['difficulty', 'avg_total_cooking_time'],
        )
        pd.testing.assert_frame_equal(expected_output, real_output,     check_like=True,check_dtype=False)

if __name__ == "__main__":
    unittest.main()