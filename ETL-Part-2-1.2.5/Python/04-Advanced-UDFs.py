# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced UDFs
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to create your own User Defined Functions (UDFs) specific to the needs of your data.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Apply UDFs with a multiple DataFrame column inputs
# MAGIC * Apply UDFs that return complex types
# MAGIC * Write vectorized UDFs using Python
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
# MAGIC src="//fast.wistia.net/embed/iframe/46zerb33vk?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/46zerb33vk?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Complex Transformations
# MAGIC  
# MAGIC UDFs provide custom, generalizable code that you can apply to ETL workloads when Spark's built-in functions won't suffice.  
# MAGIC In the last lesson we covered a simple version of this: UDFs that take a single DataFrame column input and return a primitive value. Often a more advanced solution is needed.
# MAGIC 
# MAGIC UDFs can take multiple column inputs. While UDFs cannot return multiple columns, they can return complex, named types that are easily accessible. This approach is especially helpful in ETL workloads that need to clean complex and challenging data structures.
# MAGIC 
# MAGIC Another other option is the new vectorized, or pandas, UDFs available in Spark 2.3. These allow for more performant UDFs written in Python.<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/pandas-udfs.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## KC Edit - Pandas UDF Difference

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Reading materials: https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html
# MAGIC 
# MAGIC Pandas UDF notebook: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html

# COMMAND ----------

from pyspark.sql.functions import col, rand

# Random Data Frame
df = (spark.range(0, 100 * 1000 * 1000)
  .withColumn("id", (col("id") / 1000).cast("int"))
  .withColumn("random_float", rand())
)

display(df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import udf

# Use udf to define a row-at-a-time udf
@udf('int')
def plus_one(v):
      return v + 1

df2 = df.withColumn('id2', plus_one(df.id))
display(df2)

# COMMAND ----------

# MAGIC %timeit df.withColumn('id2', plus_one(df.id))

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

# Use pandas_udf to define a Pandas UDF
@pandas_udf('double', PandasUDFType.SCALAR)
# Input/output are both a pandas.Series of doubles

def pandas_plus_one(v):
    return v + 1

df3 = df.withColumn('id2', pandas_plus_one(df.id))
display(df3)

# COMMAND ----------

# MAGIC %timeit df.withColumn('id2', pandas_plus_one(df.id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDFs with Multiple Columns
# MAGIC 
# MAGIC To begin making more complex UDFs, start by using multiple column inputs.  This is as simple as adding extra inputs to the function or lambda you convert to the UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Write a basic function that combines two columns.

# COMMAND ----------

def manual_add(x, y):
  return x + y

manual_add(1, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC Register the function as a UDF by binding it with a Python variable, adding a name to access it in the SQL API and giving it a return type.

# COMMAND ----------

from pyspark.sql.types import IntegerType

manualAddPythonUDF = spark.udf.register("manualAddSQLUDF", manual_add, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dummy DataFrame to apply the UDF.

# COMMAND ----------

integerDF = (spark.createDataFrame([
  (1, 2),
  (3, 4),
  (5, 6)
], ["col1", "col2"]))

display(integerDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the UDF to your DataFrame.

# COMMAND ----------

integerAddDF = integerDF.select("*", manualAddPythonUDF("col1", "col2").alias("sum"))

display(integerAddDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDFs with Complex Output
# MAGIC 
# MAGIC Complex outputs are helpful when you need to return multiple values from your UDF. The UDF design pattern involves returning a single column to drill down into, to pull out the desired data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Start by determining the desired output.  This will look like a schema with a high level `StructType` with numerous `StructFields`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For a refresher on this, see the lesson **Applying Schemas to JSON Data** in <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction/" target="_blank">ETL Part 1 course from Databricks Academy</a>.

# COMMAND ----------

from pyspark.sql.types import FloatType, StructType, StructField

mathOperationsSchema = StructType([
  StructField("sum", FloatType(), True), 
  StructField("multiplication", FloatType(), True), 
  StructField("division", FloatType(), True) 
])

# COMMAND ----------

# MAGIC %md
# MAGIC Create a function that returns a tuple of your desired output.

# COMMAND ----------

def manual_math(x, y):
  return (float(x + y), float(x * y), x / float(y))

manual_math(1, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC Register your function as a UDF and apply it.  In this case, your return type is the schema you created.

# COMMAND ----------

manualMathPythonUDF = spark.udf.register("manualMathSQLUDF", manual_math, mathOperationsSchema)

display(integerDF.select("*", manualMathPythonUDF("col1", "col2").alias("sum")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vectorized UDFs in Python
# MAGIC 
# MAGIC Starting in Spark 2.3, vectorized UDFs can be written in Python called Pandas UDFs.  This alleviates some of the serialization and invocation overhead of conventional Python UDFs.  While there are a number of types of these UDFs, this walk-through focuses on scalar UDFs. This is an ideal solution for Data Scientists needing performant UDFs written in Python.
# MAGIC 
# MAGIC :NOTE: Your cluster will need to run Spark 2.3 in order to execute the following code.

# COMMAND ----------

# MAGIC %md
# MAGIC Use the decorator syntax to designate a Pandas UDF.  The input and outputs are both Pandas series of doubles.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
# MAGIC 
# MAGIC @pandas_udf('double', PandasUDFType.SCALAR)
# MAGIC def pandas_plus_one(v):
# MAGIC     return v + 1

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame to apply the UDF.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col, rand
# MAGIC 
# MAGIC df = spark.range(0, 10 * 1000 * 1000)
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the UDF

# COMMAND ----------

# MAGIC %python
# MAGIC display(df.withColumn('id_transformed', pandas_plus_one("id")))

# COMMAND ----------

# MAGIC %md
# MAGIC %md ## Exercise 1: Multiple Column Inputs to Complex Type
# MAGIC 
# MAGIC Given a DataFrame of weather in various units, write a UDF that translates a column for temperature and a column for units into a complex type for temperature in three units:<br><br>
# MAGIC 
# MAGIC * fahrenheit
# MAGIC * celsius
# MAGIC * kelvin

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import and Explore the Data
# MAGIC 
# MAGIC Import the data sitting in `/mnt/training/weather/StationData/stationData.parquet` and save it to `weatherDF`.

# COMMAND ----------

# TODO
weatherDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(weatherDF.columns)

dbTest("ET2-P-04-01-01", 2559, weatherDF.count())
dbTest("ET2-P-04-01-02", True, "TAVG" in cols and "UNIT" in cols)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Define Complex Output Type
# MAGIC 
# MAGIC Define the complex output type for your UDF.  This should look like the following:
# MAGIC 
# MAGIC | Field Name | Type |
# MAGIC |:-----------|:-----|
# MAGIC | fahrenheit | Float |
# MAGIC | celsius | Float |
# MAGIC | kelvin | Float |

# COMMAND ----------

# TODO
schema = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.types import FloatType
names = [i.name for i in schema.fields]

dbTest("ET2-P-04-02-01", 3, len(schema.fields))
dbTest("ET2-P-04-02-02", [FloatType(), FloatType(), FloatType()], [i.dataType for i in schema.fields])
dbTest("ET2-P-04-02-03", True, "fahrenheit" in names and "celsius" in names and "kelvin" in names)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create the Function
# MAGIC 
# MAGIC Create a function that takes `temperature` as a Float and `unit` as a String.  `unit` will either be `F` for fahrenheit or `C` for celsius.  
# MAGIC Return a tuple of floats of that value as `(fahrenheit, celsius, kelvin)`.
# MAGIC 
# MAGIC Use the following equations:
# MAGIC 
# MAGIC | From | To Fahrenheit | To Celsius | To Kelvin |
# MAGIC |:-----|:--------------|:-----------|:-----------|
# MAGIC | Fahrenheit | F | (F - 32) * 5/9 | (F - 32) * 5/9 + 273.15 |
# MAGIC | Celsius | (C * 9/5) + 32 | C | C + 273.15 |
# MAGIC | Kelvin | (K - 273.15) * 9/5 + 32 | K - 273.15 | K |

# COMMAND ----------

# TODO
def temperatureConverter( # FILL_IN ):

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-04-03-01", (194.0, 90, 363.15), temperatureConverter(90, "C"))
dbTest("ET2-P-04-03-02", (0, -17.77777777777778, 255.3722222222222), temperatureConverter(0, "F"))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Register the UDF
# MAGIC 
# MAGIC Register the UDF as `temperatureConverterUDF`

# COMMAND ----------

# TODO
temperatureConverterUDF = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ET2-P-04-04-01", (194.0, 90, 363.15), temperatureConverterUDF.func(90, "C"))
dbTest("ET2-P-04-04-02", (0, -17.77777777777778, 255.3722222222222), temperatureConverterUDF.func(0, "F"))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Apply your UDF
# MAGIC 
# MAGIC Create `weatherEnhancedDF` with a new column `TAVGAdjusted` that applies your UDF.

# COMMAND ----------

# TODO
weatherEnhancedDF = FILL_IN

display(weatherEnhancedDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
result = weatherEnhancedDF.select("TAVGAdjusted").first()[0].asDict()

dbTest("ET2-P-04-05-01", {'fahrenheit': 61.0, 'celsius': 16.11111068725586, 'kelvin': 289.2611083984375}, result)
dbTest("ET2-P-04-05-02", 2559, weatherEnhancedDF.count())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** How do UDFs handle multiple column inputs and complex outputs?   
# MAGIC **Answer:** UDFs allow for multiple column inputs.  Complex outputs can be designated with the use of a defined schema encapsulate in a `StructType()` or a Scala case class.
# MAGIC 
# MAGIC **Question:** How can I do vectorized UDFs in Python and are they as performant as built-in functions?   
# MAGIC **Answer:** Spark 2.3 includes the use of vectorized UDFs using Pandas syntax. Even though they are vectorized, these UDFs will not be as performant built-in functions, though they will be more performant than non-vectorized Python UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Joins and Lookup Tables]($./05-Joins-and-Lookup-Tables ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more about UDFs?  
# MAGIC **A:** Take a look at the <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html" target="_blank">Databricks documentation for more details</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more about vectorized UDFs in Python?  
# MAGIC **A:** Take a look at the <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Databricks blog for more details</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more about User Defined Aggregate Functions?  
# MAGIC **A:** Take a look at the <a href="https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html" target="_blank">Databricks documentation for more details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>