import pandas as pd
import unittest
from RecipesEtl import RecipesETL
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest

class TestTime(unittest.TestCase):
    """
    Test cases to cover scenarios of failure in the conversion of ISO time to the duration 
    """
    def setUp(self):
         self.recipes_analytics = RecipesETL()
   
    def test_1_IsoCovert(self):
        iso_str = 'PT6M'
        duration = self.recipes_analytics.iso_time_to_min(iso_str)
        self.assertEqual(duration[1], 6)
    
    def test_2_IsoCovert(self):
        iso_str = 'bla bla'
        duration = self.recipes_analytics.iso_time_to_min(iso_str)
        self.assertEqual(duration[1], 0)
    
    def test_3_IsoCovert(self):
        iso_str = '    '
        duration = self.recipes_analytics.iso_time_to_min(iso_str)
        self.assertEqual(duration[1], 0)

    def test_4_IsoCovert(self):
        iso_str = ''
        duration = self.recipes_analytics.iso_time_to_min(iso_str)
        self.assertEqual(duration[1], 0)
    
       
if __name__ == "__main__":
    unittest.main()



