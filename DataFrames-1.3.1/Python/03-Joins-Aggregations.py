# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregations and JOINs
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to create on-the-fly data lakes.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use basic aggregations.
# MAGIC * Correlate two data sets with a join.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers and Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome or Firefox
# MAGIC * Lesson: <a href="$./02-Querying-Files">Querying Files with DataFrames</a>

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
# MAGIC src="//fast.wistia.net/embed/iframe/659uzjn3f5?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/659uzjn3f5?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Basic Aggregations
# MAGIC 
# MAGIC Using <a "https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions" target="_blank">built-in Spark functions</a>, you can aggregate data in various ways. 
# MAGIC 
# MAGIC Run the cell below to compute the average of all salaries in the people DataFrame.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By default, you get a floating point value.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/44ui5pgc61?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/44ui5pgc61?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/training/dataframes/"

# COMMAND ----------

peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")

# COMMAND ----------

from pyspark.sql.functions import avg
avgSalaryDF = peopleDF.select(avg("salary").alias("averageSalary"))

avgSalaryDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Convert that value to an integer using the `round()` function. See
# MAGIC <a href "https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" class="text-info">the documentation for <tt>round()</tt></a>
# MAGIC for more details.

# COMMAND ----------

from pyspark.sql.functions import round
roundedAvgSalaryDF = avgSalaryDF.select(round("averageSalary").alias("roundedAverageSalary"))

roundedAvgSalaryDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In addition to the average salary, what are the maximum and minimum salaries?

# COMMAND ----------

from pyspark.sql.functions import min, max
salaryDF = peopleDF.select(max("salary").alias("max"), min("salary").alias("min"), round(avg("salary")).alias("averageSalary"))

salaryDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Two Data Sets
# MAGIC 
# MAGIC Correlate the data in two data sets using a DataFrame join. 
# MAGIC 
# MAGIC The `people` data set has 10 million names in it. 
# MAGIC 
# MAGIC > How many of the first names appear in Social Security data files? 
# MAGIC 
# MAGIC To find out, use the Social Security data set with first name popularity data from the United States Social Security Administration. 
# MAGIC 
# MAGIC For every year from 1880 to 2014, `dbfs:/mnt/training/ssn/names-1880-2016.parquet/` lists the first names of people born in that year, their gender, and the total number of people given that name. 
# MAGIC 
# MAGIC By joining the `people` data set with `names-1880-2016`, weed out the names that aren't represented in the Social Security data.
# MAGIC 
# MAGIC (In a real application, you might use a join like this to filter out bad data.)

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/h8r3flam1s?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/h8r3flam1s?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Start by taking a look at what the social security data set looks like. Each year is its own directory.

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training/ssn/names-1880-2016.parquet/

# COMMAND ----------

# MAGIC %md
# MAGIC Let's load this file into a DataFrame and look at the data.

# COMMAND ----------

ssaDF = spark.read.parquet("/mnt/training/ssn/names-1880-2016.parquet/")

display(ssaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, with a quick count of distinct names, get an idea of how many distinct names there are in each of the tables.
# MAGIC 
# MAGIC DataFrames have a `distinct` method just for this purpose.

# COMMAND ----------

peopleDistinctNamesDF = peopleDF.select("firstName").distinct()

# COMMAND ----------

peopleDistinctNamesDF.count()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC In preparation for the join, let's rename the `firstName` column to `ssaFirstName` in the Social Security DataFrame.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Question to ponder: why would we want to do this?

# COMMAND ----------

ssaDistinctNamesDF = ssaDF.select("firstName").withColumnRenamed("firstName",'ssaFirstName').distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC Count how many distinct names in the Social Security DataFrame.

# COMMAND ----------

ssaDistinctNamesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now join the two DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col
joinedDF = peopleDistinctNamesDF.join(ssaDistinctNamesDF, col("firstName") == col("ssaFirstName"))

# COMMAND ----------

# MAGIC %md
# MAGIC How many are there?

# COMMAND ----------

joinedDF.count()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC In the tables above, some of the salaries in the `peopleDF` DataFrame are negative. 
# MAGIC 
# MAGIC These salaries represent bad data. 
# MAGIC 
# MAGIC Your job is to convert all the negative salaries to positive ones, and then sort the top 20 people by their salary.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the Apache Spark documentation, <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC Create a DataFrame`PeopleWithFixedSalariesDF`, where all the negative salaries have been converted to positive numbers.

# COMMAND ----------

peopleDF.show()

# COMMAND ----------

# TODO

from pyspark.sql.functions import abs
peopleWithFixedSalariesDF = peopleDF.withColumn("salary", abs(peopleDF.salary))

# COMMAND ----------

# TEST - Run this cell to test your solution.

belowZero = peopleWithFixedSalariesDF.filter(peopleWithFixedSalariesDF["salary"] < 0).count()
dbTest("DF-L3-belowZero", 0, belowZero)

print("Tests passed!")

# COMMAND ----------

peopleWithFixedSalariesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC 
# MAGIC Starting with the `peopleWithFixedSalariesDF` DataFrame, create another DataFrame called `PeopleWithFixedSalariesSortedDF` where:
# MAGIC 0. The data set has been reduced to the first 20 records.
# MAGIC 0. The records are sorted by the column `salary` in ascending order.

# COMMAND ----------

# TODO
peopleWithFixedSalariesSortedDF = peopleWithFixedSalariesDF.sort("salary").limit(20)

# COMMAND ----------

# TEST - Run this cell to test your solution.

resultsDF = peopleWithFixedSalariesSortedDF.select("salary")
dbTest("DF-L3-count", 20, resultsDF.count())

print("Tests passed!")

# COMMAND ----------

# TEST - Run this cell to test your solution.

from pyspark.sql import Row

results = resultsDF.collect()

dbTest("DF-L3-fixedSalaries-0", Row(salary=2), results[0])
dbTest("DF-L3-fixedSalaries-1", Row(salary=3), results[1])
dbTest("DF-L3-fixedSalaries-2", Row(salary=4), results[2])

dbTest("DF-L3-fixedSalaries-10", Row(salary=19), results[10])
dbTest("DF-L3-fixedSalaries-11", Row(salary=19), results[11])
dbTest("DF-L3-fixedSalaries-12", Row(salary=20), results[12])

dbTest("DF-L3-fixedSalaries-17", Row(salary=28), results[17])
dbTest("DF-L3-fixedSalaries-18", Row(salary=30), results[18]) 
dbTest("DF-L3-fixedSalaries-19", Row(salary=31), results[19]) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC As a refinement, assume all salaries under $20,000 represent bad rows and filter them out.
# MAGIC 
# MAGIC Additionally, categorize each person's salary into $10K groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC  Starting with the `peopleWithFixedSalariesDF` DataFrame, create a DataFrame called `peopleWithFixedSalaries20KDF` where:
# MAGIC 0. The data set excludes all records where salaries are below $20K.
# MAGIC 0. The data set includes a new column called `salary10k`, that should be the salary in groups of 10,000. For example:
# MAGIC   * A salary of 23,000 should report a value of "2".
# MAGIC   * A salary of 57,400 should report a value of "6".
# MAGIC   * A salary of 1,231,375 should report a value of "123".

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# TODO
peopleWithFixedSalaries20KDF = peopleWithFixedSalariesDF.filter(peopleWithFixedSalariesDF.salary >= 20000).withColumn("salary10k", round((peopleWithFixedSalariesDF.salary / 10000).cast(DoubleType()),0))

# COMMAND ----------

peopleWithFixedSalaries20KDF.show()

# COMMAND ----------

# TEST - Run this cell to test your solution.

below20K = peopleWithFixedSalaries20KDF.filter("salary < 20000").count()
 
dbTest("DF-L3-count-salaries", 0, below20K)  

print("Tests passed!")

# COMMAND ----------

# TEST - Run this cell to test your solution.

from pyspark.sql.functions import count
results = (peopleWithFixedSalaries20KDF 
  .select("salary10k") 
  .groupBy("salary10k") 
  .agg(count("*").alias("total")) 
  .orderBy("salary10k") 
  .limit(5) 
  .collect()
)

dbTest("DF-L3-countSalaries-0", Row(salary10k=2.0, total=43792), results[0])
dbTest("DF-L3-countSalaries-1", Row(salary10k=3.0, total=212630), results[1])
dbTest("DF-L3-countSalaries-2", Row(salary10k=4.0, total=536536), results[2])
dbTest("DF-L3-countSalaries-3", Row(salary10k=5.0, total=1055261), results[3])
dbTest("DF-L3-countSalaries-4", Row(salary10k=6.0, total=1623248), results[4])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Using the `peopleDF` DataFrame, count the number of females named Caren who were born before March 1980. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC 
# MAGIC Starting with `peopleDF`, create a DataFrame called `carensDF` where:
# MAGIC 0. The result set has a single record.
# MAGIC 0. The data set has a single column named `total`.
# MAGIC 0. The result counts only 
# MAGIC   * Females (`gender`)
# MAGIC   * First Name is "Caren" (`firstName`)
# MAGIC   * Born before March 1980 (`birthDate`)

# COMMAND ----------

peopleDF.show()

# COMMAND ----------

# TODO
carensDF = peopleDF.filter((peopleDF.gender == 'F') & (peopleDF.firstName == 'Caren') & (peopleDF.birthDate < '1980-03-01')).agg(count("*").alias("total"))

# COMMAND ----------

# TEST - Run this cell to test your solution.

rows = carensDF.collect()

dbTest("DF-L3-carens-len", 1, len(rows))
dbTest("DF-L3-carens-total", Row(total=750), rows[0])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is the DataFrame equivalent of the SQL statement `SELECT count(*) AS total`  
# MAGIC **A:** ```.agg(count("*").alias("total"))```
# MAGIC 
# MAGIC **Q:** What is the DataFrame equivalent of the SQL statement 
# MAGIC ```SELECT firstName FROM PeopleDistinctNames INNER JOIN SSADistinctNames ON firstName = ssaFirstName```  
# MAGIC **A:** 
# MAGIC `peopleDistinctNamesDF.join(ssaDistinctNamesDF, peopleDistinctNamesDF(col("firstName")) == col("ssaFirstName"))`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC * Do the [Challenge Exercise]($./Optional/03-Joins-Aggregations).
# MAGIC * Start the next lesson, [Accessing Data]($./04-Accessing-Data).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>
# MAGIC * <a href="https://databricks.com/blog/2017/08/31/cost-based-optimizer-in-apache-spark-2-2.html" target="_blank">Cost-based Optimizer in Apache Spark 2.2</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>