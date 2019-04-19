# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Data Lakes with DataFrames
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; make it easy to work with hierarchical data, such as nested JSON records.
# MAGIC 
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a Data Lake.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use DataFrames to query a Data Lake.
# MAGIC * Clean messy data sets.
# MAGIC * Join two cleaned data sets.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers and Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./02-Querying-Files">Querying Files with SQL</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lakes
# MAGIC 
# MAGIC Companies frequently store thousands of large data files gathered from various teams and departments, typically using a diverse variety of formats including CSV, JSON, and XML.  Data scientists often wish to extract insights from this data.
# MAGIC 
# MAGIC The classic approach to querying this data is to load it into a central database called a <b>data warehouse</b>.  Traditionally, data engineers must design the schema for the central database, extract the data from the various data sources, transform the data to fit the warehouse schema, and load it into the central database.
# MAGIC A data scientist can then query the data warehouse directly or query smaller data sets created to optimize specific types of queries.
# MAGIC 
# MAGIC The classic data warehouse approach works well but requires a great deal of up front effort to design and populate schemas.  It also limits historical data, which is constrained to only the data that fits the warehouse’s schema.
# MAGIC 
# MAGIC An alternative approach is a <b>Data Lake</b>, which:
# MAGIC 
# MAGIC * Is a storage repository that cheaply stores a vast amount of raw data in its native format.
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * May contain operational relational databases with live transactional data.
# MAGIC 
# MAGIC Spark is ideal for querying Data Lakes. Use Spark DataFrames to read directly from raw files and then execute queries to join and aggregate the data.
# MAGIC 
# MAGIC This lesson illustrates that once two DataFrames are created (independent of their underlying file type), we can perform a variety of operations on them including joins, nested queries, and many others.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/xtpxgg1cbv?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xtpxgg1cbv?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at our Data Lake
# MAGIC 
# MAGIC Start by reviewing which files are in our Data Lake.
# MAGIC 
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are Parquet files containing 2016 crime data from several United States cities.
# MAGIC 
# MAGIC In the cell below we have data for Boston, Chicago, New Orleans, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/l9g7bdhfle?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/l9g7bdhfle?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %fs ls /mnt/training/crime-data-2016

# COMMAND ----------

# MAGIC %md
# MAGIC The next step in looking at the data is to create a DataFrame for each file.  
# MAGIC 
# MAGIC Start by creating a DataFrame of the data from New York and then Boston:
# MAGIC 
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **New York**  | `CrimeDataNewYork`      | `dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet`
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet`

# COMMAND ----------

crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")

# COMMAND ----------

crimeDataBostonDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC With the two DataFrames created, it is now possible to review the first couple records of each file.
# MAGIC 
# MAGIC Notice in the example below:
# MAGIC * The `crimeDataNewYorkDF` and `crimeDataBostonDF` DataFrames use different names for the columns.
# MAGIC * The data itself is formatted differently and different names are used for similar concepts.
# MAGIC 
# MAGIC This is common in a Data Lake. Often files are added to a Data Lake by different groups at different times. The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data. The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a traditional relational data model, so the person querying the Data Lake will need to normalize data before extracting useful insights.
# MAGIC 
# MAGIC The alternative to a Data Lake is a data warehouse.  In a data warehouse, a committee often regulates the schema and ensures data is normalized before being made available.  This makes querying much easier but also makes gathering the data much more expensive and time-consuming.  Many companies choose to start with a Data Lake to accumulate data.  Then, as the need arises, they normalize data and produce higher quality tables for querying.  This reduces the up front costs while still making data easier to query over time.  The normalized tables can later be loaded into a formal data warehouse through nightly batch jobs.  In this way, Apache Spark is used to manage and query both Data Lakes and data warehouses.

# COMMAND ----------

display(crimeDataNewYorkDF)

# COMMAND ----------

display(crimeDataBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same Type of Data, Different Structure
# MAGIC 
# MAGIC In this section, we examine crime data to determine how to extract homicide statistics.
# MAGIC 
# MAGIC Because the data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/ah2qrd2nh6?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/ah2qrd2nh6?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC 
# MAGIC At the same time, normalize the data structure of each table so all the columns (and their values) line up with each other.
# MAGIC 
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC 
# MAGIC For the upcoming aggregation, you need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC In this example, we use several functions in the `pyspark.sql.functions` library, and need to import:
# MAGIC 
# MAGIC * `month()` to extract the month from `reportDate` timestamp data type.
# MAGIC * `lower()` to convert text to lowercase.
# MAGIC * `contains(mySubstr)` to indicate a string contains substring `mySubstr`.
# MAGIC 
# MAGIC Also, note we use  `|`  to indicate a logical `or` of two conditions in the `filter` method.

# COMMAND ----------

from pyspark.sql.functions import lower, upper, month, col

homicidesNewYorkDF = (crimeDataNewYorkDF 
  .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense")) 
  .filter(lower(col("offenseDescription")).contains("murder") | lower(col("offenseDescription")).contains("homicide"))
)

display(homicidesNewYorkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how the same kind of information is presented differently in the Boston data:
# MAGIC 
# MAGIC `offense` is called `OFFENSE_CODE_GROUP` and there is only one category `homicide`.

# COMMAND ----------

homicidesBostonDF = (crimeDataBostonDF 
  .select("month", col("OFFENSE_CODE_GROUP").alias("offense")) 
  .filter(lower(col("OFFENSE_CODE_GROUP")).contains("homicide"))
)

display(homicidesBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC See below the structure of the two tables is now identical.

# COMMAND ----------

display(homicidesNewYorkDF.limit(5))

# COMMAND ----------

display(homicidesBostonDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing the Data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now that you normalized the homicide data for each city, combine the two by taking their union.
# MAGIC 
# MAGIC When done, aggregate that data to compute the number of homicides per month.
# MAGIC 
# MAGIC Start by creating a new DataFrame called `homicidesBostonAndNewYorkDF` that consists of the `union` of `homicidesNewYorkDF` with `homicidesBostonDF`.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/d76wihpsh6?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/d76wihpsh6?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

homicidesBostonAndNewYorkDF = homicidesNewYorkDF.union(homicidesBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC See all the data in one table below:

# COMMAND ----------

display(homicidesBostonAndNewYorkDF.orderBy("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC And finally, perform a simple aggregation to see the number of homicides per month:

# COMMAND ----------

display(homicidesBostonAndNewYorkDF.select("month").orderBy("month").groupBy("month").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Merge the crime data for Chicago with the data for New York and Boston, and then update our final aggregation of counts-by-month.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC 
# MAGIC Create the initial DataFrame of the Chicago data.
# MAGIC 0. The source file is `dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet`.
# MAGIC 0. Name the view `crimeDataChicagoDF`.
# MAGIC 0. View the data by invoking the `show()` method.

# COMMAND ----------

# TODO

crimeDataChicagoDF = spark.read.parquet('dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet')
display(crimeDataChicagoDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.

total = crimeDataChicagoDF.count()

dbTest("DF-L6-crimeDataChicago-count", 267872, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new view that normalizes the data structure.
# MAGIC 0. Name the DataFrame `homicidesChicagoDF`.
# MAGIC 0. The DataFrame should have at least two columns: `month` and `offense`.
# MAGIC 0. Filter the data to include only homicides.
# MAGIC 0. View the data by invoking the `show()` method.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the `month()` function to extract the month-of-the-year.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To find out which values for each offense constitutes a homicide, produce a distinct list of values from the`crimeDataChicagoDF` DataFrame.

# COMMAND ----------

# Get unique values
display(crimeDataChicagoDF.select("primaryType").distinct())

# COMMAND ----------

# TODO

homicidesChicagoDF = (crimeDataChicagoDF
                      .select(month(col("date")).alias("month"), col("primaryType").alias("offense"))
                      .filter(lower(col("offense")).contains("homicide"))
                     )

display(homicidesChicagoDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
homicidesChicago = homicidesChicagoDF.select("month").groupBy("month").count().orderBy("month").collect()

dbTest("DF-L6-homicideChicago-len", 12, len(homicidesChicago))
dbTest("DF-L6-homicideChicago-0", 54, homicidesChicago[0][1])
dbTest("DF-L6-homicideChicago-6", 71, homicidesChicago[6][1])
dbTest("DF-L6-homicideChicago-11", 58, homicidesChicago[11][1])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Create a new DataFrame that merges all three data sets (New York, Boston, Chicago):
# MAGIC 0. Name the view `allHomicidesDF`.
# MAGIC 0. Use the `union()` method introduced earlier to merge all three tables.
# MAGIC   * `homicidesNewYorkDF`
# MAGIC   * `homicidesBostonDF`
# MAGIC   * `homicidesChicagoDF`
# MAGIC 0. View the data by invoking the `show()` method.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To union three tables together, copy the previous example and apply a `union()` method again.

# COMMAND ----------

# TODO

allHomicidesDF = (homicidesNewYorkDF
                  .union(homicidesBostonDF)
                  .union(homicidesChicagoDF)
                 )

display(allHomicidesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.

allHomicides = allHomicidesDF.count()
dbTest("DF-L6-allHomicides-count", 1203, allHomicides)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4
# MAGIC 
# MAGIC Create a new DataFrame that counts the number of homicides per month.
# MAGIC 0. Name the DataFrame `homicidesByMonthDF`.
# MAGIC 0. Rename the column `count(1)` to `homicides`.
# MAGIC 0. Group the data by `month`.
# MAGIC 0. Sort the data by `month`.
# MAGIC 0. Count the number of records for each aggregate.
# MAGIC 0. View the data by invoking the `show()` method.

# COMMAND ----------

# TODO
from pyspark.sql.functions import count

homicidesByMonthDF  = (allHomicidesDF
                      .groupby("month")
                      .count().alias("homicides")
                      .sort("month"))

display(homicidesByMonthDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
allHomicides = homicidesByMonthDF.collect()

dbTest("DF-L6-homicidesByMonth-len", 12, len(allHomicides))
dbTest("DF-L6-homicidesByMonth-0", 1, allHomicides[0][0])
dbTest("DF-L6-homicidesByMonth-11", 12, allHomicides[11][0])
dbTest("DF-L6-allHomicides-0", 83, allHomicides[0][1])
dbTest("DF-L6-allHomicides-1", 83, allHomicides[0][1])
dbTest("DF-L6-allHomicides-2", 68, allHomicides[1][1])
dbTest("DF-L6-allHomicides-3", 72, allHomicides[2][1])
dbTest("DF-L6-allHomicides-4", 76, allHomicides[3][1])
dbTest("DF-L6-allHomicides-5", 105, allHomicides[4][1])
dbTest("DF-L6-allHomicides-6", 120, allHomicides[5][1])
dbTest("DF-L6-allHomicides-7", 116, allHomicides[6][1])
dbTest("DF-L6-allHomicides-8", 144, allHomicides[7][1])
dbTest("DF-L6-allHomicides-9", 109, allHomicides[8][1])
dbTest("DF-L6-allHomicides-10", 109, allHomicides[9][1])
dbTest("DF-L6-allHomicides-11", 111, allHomicides[10][1])
dbTest("DF-L6-allHomicides-12", 90, allHomicides[11][1])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC * Spark DataFrames allow you to easily manipulate data in a Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is a Data Lake?  
# MAGIC **A:** A Data Lake is a collection of data files gathered from various sources.  Spark loads each file as a table and then executes queries by joining and aggregating these files.
# MAGIC 
# MAGIC **Q:** What are advantages of Data Lakes over classic Data Warehouses?  
# MAGIC **A:** Data Lakes allow for large amounts of data to be aggregated from many sources with minimal preparatory steps.  Data Lakes also allow for very large files.  Powerful query engines such as Spark can read the diverse collection of files and execute complex queries efficiently.
# MAGIC 
# MAGIC **Q:** What are some advantages of Data Warehouses?  
# MAGIC **A:** Data warehouses are neatly curated to ensure data from all sources fit a common schema.  This makes them easy to query.
# MAGIC 
# MAGIC **Q:** What's the best way to combine the advantages of Data Lakes and Data Warehouses?  
# MAGIC **A:** Start with a Data Lake.  As you query, you will discover cases where the data needs to be cleaned, combined, and made more accessible.  Create periodic Spark jobs to read these raw sources and write new "golden" DataFrames that are cleaned and more easily queried.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Please complete this short [feedback survey](https://www.surveymonkey.com/r/WH9L2LX).  Your input is extremely important and will shape future development.
# MAGIC * Next, take what you learned about working data lakes and apply them in the [Capstone Project]($./07-Capstone-Project).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>