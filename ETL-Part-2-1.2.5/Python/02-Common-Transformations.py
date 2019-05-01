# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Common Transformations
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to manipulate data with built-in functions that accommodate common design patterns.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Apply built-in functions to manipulate data
# MAGIC * Define logic to handle null values
# MAGIC * Deduplicate a data set
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Please use a <a href="https://docs.azuredatabricks.net/user-guide/supported-browsers.html#supported-browsers" target="_blank">supported browser</a>.
# MAGIC * Concept (optional): <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/xjbyksd137?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xjbyksd137?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Transformations in ETL
# MAGIC 
# MAGIC The goal of transformations in ETL is to transform raw data in order to populate a data model.  The most common models are **relational models** and **snowflake (or star) schemas,** though other models such as query-first modeling also exist. Relational modeling entails distilling your data into efficient tables that you can join back together. A snowflake model is generally used in data warehousing where a fact table references any number of related dimension tables. Regardless of the model you use, the ETL approach is generally the same.
# MAGIC 
# MAGIC Transforming data can range in complexity from simply parsing relevant fields to handling null values without affecting downstream operations and applying complex conditional logic.  Common transformations include:<br><br>
# MAGIC 
# MAGIC * Normalizing values
# MAGIC * Imputing null or missing data
# MAGIC * Deduplicating data
# MAGIC * Performing database rollups
# MAGIC * Exploding arrays
# MAGIC * Pivoting DataFrames
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/data-models.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Built-In Functions
# MAGIC 
# MAGIC Built-in functions offer a range of performant options to manipulate data. This includes options familiar to:<br><br>
# MAGIC 
# MAGIC 1. SQL users such as `.select()` and `.groupBy()`
# MAGIC 2. Python, Scala and R users such as `max()` and `sum()`
# MAGIC 3. Data warehousing options such as `rollup()` and `cube()`
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** For more depth on built-in functions, see  <a href="https://academy.databricks.com/collections/frontpage/products/dataframes" target="_blank">Getting Started with Apache Spark DataFrames course from Databricks Academy</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster. Click <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalizing Data
# MAGIC 
# MAGIC Normalizing refers to different practices including restructuring data in normal form to reduce redundancy, and scaling data down to a small, specified range. For this case, bound a range of integers between 0 and 1.

# COMMAND ----------

# MAGIC %md
# MAGIC Start by taking a DataFrame of a range of integers

# COMMAND ----------

integerDF = spark.range(1000, 10000)

display(integerDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC To normalize these values between 0 and 1, subtract the minimum and divide by the maximum, minus the minimum.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=minmaxscaler#pyspark.ml.feature.MinMaxScaler" target="_blank">Also see the built-in class `MinMaxScaler`</a>

# COMMAND ----------

from pyspark.sql.functions import col, max, min

colMin = integerDF.select(min("id")).first()[0]
colMax = integerDF.select(max("id")).first()[0]

normalizedIntegerDF = (integerDF
  .withColumn("normalizedValue", (col("id") - colMin) / (colMax - colMin) )
)

display(normalizedIntegerDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Imputing Null or Missing Data
# MAGIC 
# MAGIC Null values refer to unknown or missing data as well as irrelevant responses. Strategies for dealing with this scenario include:<br><br>
# MAGIC 
# MAGIC * **Dropping these records:** Works when you do not need to use the information for downstream workloads
# MAGIC * **Adding a placeholder (e.g. `-1`):** Allows you to see missing data later on without violating a schema
# MAGIC * **Basic imputing:** Allows you to have a "best guess" of what the data could have been, often by using the mean of non-missing data
# MAGIC * **Advanced imputing:** Determines the "best guess" of what data should be using more advanced strategies such as clustering machine learning algorithms or oversampling techniques 
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=imputer#pyspark.ml.feature.Imputer" target="_blank">Also see the built-in class `Imputer`</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the following DataFrame, which has missing values.

# COMMAND ----------

corruptDF = spark.createDataFrame([
  (11, 66, 5),
  (12, 68, None),
  (1, None, 6),
  (2, 72, 7)], 
  ["hour", "temperature", "wind"]
)

display(corruptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop any records that have null values.

# COMMAND ----------

corruptDroppedDF = corruptDF.dropna("any")

display(corruptDroppedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Impute values with the mean.

# COMMAND ----------

corruptImputedDF = corruptDF.na.fill({"temperature": 68, "wind": 6})

display(corruptImputedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicating Data
# MAGIC 
# MAGIC Duplicate data comes in many forms. The simple case involves records that are complete duplicates of another record. The more complex cases involve duplicates that are not complete matches, such as matches on one or two columns or "fuzzy" matches that account for formatting differences or other non-exact matches. 

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the following DataFrame that has duplicate values.

# COMMAND ----------

duplicateDF = spark.createDataFrame([
  (15342, "Conor", "red"),
  (15342, "conor", "red"),
  (12512, "Dorothy", "blue"),
  (5234, "Doug", "aqua")], 
  ["id", "name", "favorite_color"]
)

display(duplicateDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop duplicates on `id` and `favorite_color`.

# COMMAND ----------

duplicateDedupedDF = duplicateDF.dropDuplicates(["id", "favorite_color"])

display(duplicateDedupedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Helpful Data Manipulation Functions
# MAGIC 
# MAGIC | Function    | Use                                                                                                                        |
# MAGIC |:------------|:---------------------------------------------------------------------------------------------------------------------------|
# MAGIC | `explode()` | Returns a new row for each element in the given array or map                                                               |
# MAGIC | `pivot()`   | Pivots a column of the current DataFrame and perform the specified aggregation                                             |
# MAGIC | `cube()`    | Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregation on them   |
# MAGIC | `rollup()`  | Create a multi-dimensional rollup for the current DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Deduplicating Data
# MAGIC 
# MAGIC A common ETL workload involves cleaning duplicated records that don't completely match up.  The source of the problem can be anything from user-generated content to schema evolution and data corruption.  Here, you match records and reduce duplicate records. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1: Import and Examine the Data
# MAGIC 
# MAGIC The file is sitting in `/mnt/training/dataframes/people-with-dups.txt`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You have to deal with the header and delimiter.

# COMMAND ----------

# MAGIC %md
# MAGIC http://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader

# COMMAND ----------

# TODO
dupedDF = (spark.read
           .option("header", True)
           .option("inferSchema", True)
           .option("sep",":")
           .csv('/mnt/training/dataframes/people-with-dups.txt'))
display(dupedDF.limit(5))

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(dupedDF.columns)

dbTest("ET2-P-02-01-01", 103000, dupedDF.count())
dbTest("ET2-P-02-01-02", True, "salary" in cols and "lastName" in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Add Columns to Filter Duplicates
# MAGIC 
# MAGIC Add columns following to allow you to filter duplicate values.  Add the following:
# MAGIC 
# MAGIC - `lcFirstName`: first name lower case
# MAGIC - `lcLastName`: last name lower case
# MAGIC - `lcMiddleName`: middle name lower case
# MAGIC - `ssnNums`: social security number without hyphens between numbers
# MAGIC 
# MAGIC Save the results to `dupedWithColsDF`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the Spark function `lower()`

# COMMAND ----------

# TODO
dupedWithColsDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(dupedWithColsDF.columns)

dbTest("ET2-P-02-02-01", 103000, dupedWithColsDF.count())
dbTest("ET2-P-02-02-02", True, "lcFirstName" in cols and "lcLastName" in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Deduplicate the Data
# MAGIC 
# MAGIC Deduplicate the data by dropping duplicates of all records except for the original names (first, middle, and last) and the original `ssn`.  Save the result to `dedupedDF`.  Drop the columns you added in step 2.

# COMMAND ----------

# TODO
dedupedDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(dedupedDF.columns)

dbTest("ET2-P-02-03-01", 100000, dedupedDF.count())
dbTest("ET2-P-02-03-02", 7, len(cols))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What built-in functions are available in Spark?  
# MAGIC **Answer:** Built-in functions include SQL functions, common programming language primitives, and data warehousing specific functions.  See the Spark API Docs for more details. (<a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python</a> or <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package" target="_blank">Scala</a>).
# MAGIC 
# MAGIC **Question:** What's the best way to handle null values?  
# MAGIC **Answer:** The answer depends largely on what you hope to do with your data moving forward. You can drop null values or impute them with a number of different techniques.  For instance, clustering your data to fill null values with the values of nearby neighbors often gives more insight to machine learning models than using a simple mean.
# MAGIC 
# MAGIC **Question:** What are potential challenges of deduplicating data and imputing null values?  
# MAGIC **Answer:** Challenges include knowing which is the correct record to keep and how to define logic that applies to the root cause of your situation. This decision making process depends largely on how removing or imputing data will affect downstream operations like database queries and machine learning workloads. Knowing the end application of the data helps determine the best strategy to use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [User Defined Functions]($./03-User-Defined-Functions ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** How can I do ACID transactions with Spark?  
# MAGIC **A:** ACID compliance refers to a set of properties of database transactions that guarantee the validity of you data.  <a href="https://databricks.com/product/databricks-delta" target="_blank">Databricks Delta</a> is an ACID compliant solution to transactionality with Spark workloads.
# MAGIC 
# MAGIC **Q:** How can I handle more complex conditional logic in Spark?  
# MAGIC **A:** You can handle more complex if/then conditional logic using the `when()` function and its `.otherwise()` method.
# MAGIC 
# MAGIC **Q:** How can I handle data warehousing functions like rollups?  
# MAGIC **A:** Spark allows for rollups and cubes, which are common in star schemas, using the `rollup()` and `cube()` functions.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>