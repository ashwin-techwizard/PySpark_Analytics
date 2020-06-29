# HelloFresh Data Engineering Test

Thank you for your interest in joining HelloFresh! As part of our selection process, all of our candidates must take the following test.
The test is designed to assess key competencies required in your role as a data engineer at HelloFresh.

Please submit your answers in a different branch and create a pull request. Please do not merge your own pull request.

_Note: While we love open source here at HelloFresh, please do not create a public repo with your test in! This challenge is only shared with people interviewing, and for obvious reasons we'd like it to remain this way._


# HelloFresh
At HelloFresh, our mission is to change the way people eat - forever. From our 2011 founding in Europe’s vibrant tech hub Berlin, we’ve become the global market leader in the meal kit sector and inspire millions of energized home cooks across the globe every week.
We offer our meal kit boxes full of exciting recipes and thoughtfully sourced, fresh ingredients in more than 13 countries, operating from offices in Berlin, New York City, Sydney, Toronto, London, Amsterdam and Copenhagen and shipped out more than 250 Million meals in 2019.
Data Engineering at HelloFresh
We ingest events from our Kafka Stream and store them in our DataLake on s3. 
Events are sorted by arriving date. For example `events/recipe_changes/2019/11/29`.
During events processing we heavily rely on execution day to make sure we pick proper chunk of data and keep historical results.
We use Apache Spark to work with data and store it on s3 in parquet format. Our primary programming language is Python.

# Exercise
## Overview
At HelloFresh we have a big recipes archive that was created over the last 8 years. 
It is constantly being updated either by adding new recipes or by making changes to existing ones. 
We have a service that can dump archive in JSON format to selected s3 location. 
We are interested in tracking changes to see available recipes, their cooking time and difficulty level.

## Task 1
Using Apache Spark and Python, read and pre-process rows to ensure further optimal structure and performance 
for further processing. 
Use the dataset on S3 as the input (https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json). It's fine to download it locally.

## Task 2
Using Apache Spark and Python read processed dataset from step 1 and: 
1. extract only recipes that have `beef` as one of the ingredients
2. calculate average cooking time duration per difficulty level

Total cooking time duration can be calculated by formula:
```bash
total_cook_time = cookTime + prepTime
```  

Criteria for levels based on total cook time duration:
- easy - less than 30 mins
- medium - between 30 and 60 mins
- hard - more than 60 mins.

## Deliverables
- A deployable Spark Application written in Python
- a README file with brief explanation of approach, data exploration and assumptions/considerations. 
You can use this file by adding new section or create a new one.
- a CSV file with average cooking time per difficulty level. Please add it to `output` folder.
File should have 2 columns: `difficulty,avg_total_cooking_time` and named as `report.csv`

## Requirements
- Well structured, object-oriented, documented and maintainable code
- Unit tests to test the different components
- Errors handling
- Documentation
- Solution is deployable and we can run it

## Bonus points
- Config handling
- Logging and alerting
- Consider scaling of your application
- CI/CD explained
- Performance tuning explained
- We love clean and maintainable code
- We appreciate good combination of Software and Data Engineering

Good Luck!
