# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # User Defined Functions
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to create your own User Defined Functions (UDFs) specific to the needs of your data.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Write UDFs with a single DataFrame column inputs
# MAGIC * Transform data using UDFs using both the DataFrame and SQL API
# MAGIC * Analyze the performance trade-offs between built-in functions and UDFs
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
# MAGIC src="//fast.wistia.net/embed/iframe/fgp5h61gps?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/fgp5h61gps?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Custom Transformations with User Defined Functions
# MAGIC 
# MAGIC Spark's built-in functions provide a wide array of functionality, covering the vast majority of data transformation use cases. Often what differentiates strong Spark programmers is their ability to utilize built-in functions since Spark offers many highly optimized options to manipulate data. This matters for two reasons:<br><br>
# MAGIC 
# MAGIC - First, *built-in functions are finely tuned* so they run faster than less efficient code provided by the user.  
# MAGIC - Secondly, Spark (or, more specifically, Spark's optimization engine, the Catalyst Optimizer) knows the objective of built-in functions so it can *optimize the execution of your code by changing the order of your tasks.* 
# MAGIC 
# MAGIC In brief, use built-in functions whenever possible.
# MAGIC 
# MAGIC There are, however, many specific use cases not covered by built-in functions. **User Defined Functions (UDFs) are useful when you need to define logic specific to your use case and when you need to encapsulate that solution for reuse.** They should only be used when there is no clear way to accomplish a task using built-in functions.
# MAGIC 
# MAGIC UDFs are generally more performant in Scala than Python since for Python, Spark has to spin up a Python interpreter on every executor to run the function. This causes a substantial performance bottleneck due to communication across the Py4J bridge (how the JVM inter-operates with Python) and the slower nature of Python execution.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/built-in-vs-udfs.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Basic UDF
# MAGIC 
# MAGIC UDFs take a function or lambda and make it available for Spark to use.  Start by writing code in your language of choice that will operate on a single row of a single column in your DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Write a basic function that splits a string on an `e`.

# COMMAND ----------

def manual_split(x):
  return x.split("e")

manual_split("this is my example string")

# COMMAND ----------

# MAGIC %md
# MAGIC Register the function as a UDF by designating the following:
# MAGIC 
# MAGIC * A name for access in Python (`manualSplitPythonUDF`)
# MAGIC * A name for access in SQL (`manualSplitSQLUDF`)
# MAGIC * The function itself (`manual_split`)
# MAGIC * The return type for the function (`StringType`)

# COMMAND ----------

from pyspark.sql.types import StringType

manualSplitPythonUDF = spark.udf.register("manualSplitSQLUDF", manual_split, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame of 100k values with a string to index. Do this by using a hash function, in this case `SHA-1`.

# COMMAND ----------

from pyspark.sql.functions import sha1, rand
randomDF = (spark.range(1, 10000 * 10 * 10 * 10)
  .withColumn("random_value", rand(seed=10).cast("string"))
  .withColumn("hash", sha1("random_value"))
  .drop("random_value")
)

display(randomDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the UDF by using it just like any other Spark function.

# COMMAND ----------

randomAugmentedDF = randomDF.select("*", manualSplitPythonUDF("hash").alias("augmented_col"))

display(randomAugmentedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame and SQL APIs
# MAGIC 
# MAGIC When you registered the UDF, it was named `manualSplitSQLUDF` for access in the SQL API. This gives us the same access to the UDF you had in the python DataFrames API.

# COMMAND ----------

# MAGIC %md
# MAGIC Register `randomDF` to access it within SQL.

# COMMAND ----------

randomDF.createOrReplaceTempView("randomTable")

# COMMAND ----------

# MAGIC %md
# MAGIC Now switch to the SQL API and use the same UDF.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id,
# MAGIC   hash,
# MAGIC   manualSplitSQLUDF(hash) as augmented_col
# MAGIC FROM
# MAGIC   randomTable

# COMMAND ----------

# MAGIC %md
# MAGIC This is an easy way to generalize UDFs, allowing teams to share their code.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Trade-offs
# MAGIC 
# MAGIC The performance of custom UDFs normally trail far behind built-in functions.  Take a look at this other example to compare built-in functions to custom UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a large DataFrame of random values, cache the result in order to keep the DataFrame in memory, and perform a `.count()` to trigger the cache to take effect.

# COMMAND ----------

from pyspark.sql.functions import col, rand

randomFloatsDF = (spark.range(0, 100 * 1000 * 1000)
  .withColumn("id", (col("id") / 1000).cast("integer"))
  .withColumn("random_float", rand())
)

randomFloatsDF.cache()
randomFloatsDF.count()

display(randomFloatsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Register a new UDF that increments a column by 1.  Here, use a lambda instead of a function.

# COMMAND ----------

from pyspark.sql.types import FloatType
  
plusOneUDF = spark.udf.register("plusOneUDF", lambda x: x + 1, FloatType())

# COMMAND ----------

# MAGIC %md
# MAGIC Compare the results using the `%timeit` function.  Run it a few times and examine the results.

# COMMAND ----------

# MAGIC %timeit randomFloatsDF.withColumn("incremented_float", plusOneUDF("random_float")).count()

# COMMAND ----------

# MAGIC %timeit randomFloatsDF.withColumn("incremented_float", col("random_float") + 1).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Which was faster, the UDF or the built-in functionality?  By how much?  This can differ based upon whether you work through this course in Scala (which is much faster) or Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Converting IP Addresses to Decimals
# MAGIC 
# MAGIC Write a UDF that translates an IPv4 address string (e.g. `123.123.123.123`) into a numeric value.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Function
# MAGIC 
# MAGIC IP addresses pose challenges for efficient database lookups.  One way of dealing with this is to store an IP address in numerical form.  Write the function `IPConvert` that satisfies the following:
# MAGIC 
# MAGIC Input: IP address as a string (e.g. `123.123.123.123`)  
# MAGIC Output: an integer representation of the IP address (e.g. `2071690107`)
# MAGIC 
# MAGIC If the input string is `1.2.3.4`, think of it like `A.B.C.D` where A is 1, B is 2, etc. Solve this with the following steps:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp; (A x 256^3) + (B x 256^2) + (C x 256) + D <br>
# MAGIC &nbsp;&nbsp;&nbsp; (1 x 256^3) + (2 x 256^2) + (3 x 256) + 4 <br>
# MAGIC &nbsp;&nbsp;&nbsp; 116777216 + 131072 + 768 + 4 <br>
# MAGIC &nbsp;&nbsp;&nbsp; 16909060
# MAGIC 
# MAGIC Make a function to implement this.

# COMMAND ----------

# TODO
def IPConvert(IPString):
  FILL_IN

IPConvert("1.2.3.4") # should equal 16909060

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-03-01-01", 16909060, IPConvert("1.2.3.4"))
dbTest("ET2-P-03-01-02", 168430090, IPConvert("10.10.10.10"))
dbTest("ET2-P-03-01-03", 386744599, IPConvert("23.13.65.23"))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Register a UDF
# MAGIC 
# MAGIC Register your function as `IPConvertUDF`.  Be sure to use `LongType` as your output type.

# COMMAND ----------

# TODO
IPConvertUDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
testDF = spark.createDataFrame((
  ("1.2.3.4", ),
  ("10.10.10.10", ),
  ("23.13.65.23", )
), ("ip",))
result = [i[0] for i in testDF.select(IPConvertUDF("ip")).collect()]

dbTest("ET2-P-03-02-01", 16909060, result[0])
dbTest("ET2-P-03-02-02", 168430090, result[1])
dbTest("ET2-P-03-02-03", 386744599, result[2])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Apply the UDF
# MAGIC 
# MAGIC Apply the UDF on the `IP` column of the DataFrame created below, creating the new column `parsedIP`.

# COMMAND ----------

# TODO
IPDF = spark.createDataFrame([["123.123.123.123"], ["1.2.3.4"], ["127.0.0.0"]], ['ip'])

IPDFWithParsedIP = # FILL_IN

display(IPDFWithParsedIP)

# COMMAND ----------

# TEST - Run this cell to test your solution
result2 = [i[1] for i in IPDFWithParsedIP.collect()]

dbTest("ET2-P-03-03-01", 2071690107, result2[0])
dbTest("ET2-P-03-03-02", 16909060, result2[1])
dbTest("ET2-P-03-03-03", 2130706432, result2[2])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What are the performance trade-offs between UDFs and built-in functions?  When should I use each?  
# MAGIC **Answer:** Built-in functions are normally faster than UDFs and should be used when possible.  UDFs should be used when specific use cases arise that aren't addressed by built-in functions.
# MAGIC 
# MAGIC **Question:** How can I use UDFs?  
# MAGIC **Answer:** UDFs can be used in any Spark API. They can be registered for use in SQL and can otherwise be used in Scala, Python, R, and Java.
# MAGIC 
# MAGIC **Question:** Why are built-in functions faster?  
# MAGIC **Answer:** Reasons include:
# MAGIC * The catalyst optimizer knows how to optimize built-in functions
# MAGIC * They are written in highly optimized Scala
# MAGIC * There is no serialization cost at the time of running a built-in function
# MAGIC 
# MAGIC **Question:** Can UDFs have multiple column inputs and outputs?  
# MAGIC **Answer:** Yes, UDFs can have multiple column inputs and multiple complex outputs. This is covered in the following lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Advanced UDFs]($./04-Advanced-UDFs ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more about UDFs?  
# MAGIC **A:** Take a look at the <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html" target="_blank">Databricks documentation for more details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>