# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Capstone Project: An End-to-End ML Model
# MAGIC 
# MAGIC The goal of this project is to build an end-to-end machine learning model from data exploration and featurization through model training and hyperparameter tuning.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Prerequisites
# MAGIC * Lesson: <a href="$./04-Exploratory-Analysis">Exploratory Analysis</a> 
# MAGIC * Lesson: <a href="$./05-Featurization">Featurization</a> 
# MAGIC * Lesson: <a href="$./06-Regression-Modeling">Regression Modeling</a> 
# MAGIC * Lesson: <a href="$./08-Model-Selection">Model Selection</a> 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC This exercise elaborates on the Airbnb dataset used in the lesson on featurization.  The goals are as follows:
# MAGIC 
# MAGIC 1. Perform exploratory analysis
# MAGIC 2. Featurize the dataset
# MAGIC 3. Train a linear regression model
# MAGIC 4. Tune the model hyperparameters using grid search and cross-validation
# MAGIC 
# MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="http://insideairbnb.com/get-the-data.html" target="_blank">You can find more information on the dataset here.</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to setup the environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Exploratory Analysis
# MAGIC 
# MAGIC Import the dataset and perform exploratory analysis including calculating summary statistics and plotting histograms, a scatterplot, and a heatmap of the city of San Francisco.

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Import and Summary Statistics
# MAGIC 
# MAGIC The dataset has been cleaned for you based on the exercise from the featurization lesson.  Import the data and calculate summary statistics.

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data, sitting in `/mnt/training/airbnb/sf-listings/sf-listings-clean.parquet`

# COMMAND ----------

filePath = "/mnt/training/airbnb/sf-listings/sf-listings-clean.parquet"

initDF = spark.read.parquet(filePath)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate summary statistics on the data to answer the following questions: <br><br>
# MAGIC 
# MAGIC 1. What do each of the variables mean?
# MAGIC 2. Are there missing values?
# MAGIC 3. How many records are in the dataset?
# MAGIC 4. What is the mean and standard deviation of the dataset?
# MAGIC 5. Which variables are continuous and which are categorical?

# COMMAND ----------

display(initDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Make a new DataFrame `airbnbDF` that consists of `initDF` without the column `price_raw`.  Cache the results.

# COMMAND ----------

# TODO
airbnbDF = # FILL_IN
airbnbDF.cache()

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-01-01", True, airbnbDF.count() == 4759)
dbTest("ML1-P-09-01-02", False, "price_raw" in airbnbDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Plotting Histograms and Scatterplots
# MAGIC 
# MAGIC Create a histogram of price

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Is this a <a href="https://en.wikipedia.org/wiki/Log-normal_distribution" target="_blank">Log Normal</a> distribution? Take the `log` of price and check the histogram.

# COMMAND ----------

# TODO
from pyspark.sql.functions import log

display( < FILL_IN > )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Create a scattermatrix (a matrix of scatterplots) to look at how the following variables correlate together:<br><br>
# MAGIC 
# MAGIC * `price`
# MAGIC * `host_total_listings_count`
# MAGIC * `review_scores_rating`
# MAGIC * `number_of_reviews`
# MAGIC * `bathrooms`
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can do this with the built-in plotting functionality or with your favorite plotting library.  Libraries like `matplotlib` and `seaborn` come pre-installed in Databricks clusters.

# COMMAND ----------

# TODO
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Which neighborhoods have the highest number of rentals? Display the neighborhoods and their associated count in descending order.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Plotting on a Map
# MAGIC 
# MAGIC Map the various listings by price.  Since Databricks notebooks can display arbitrary HTML code, use the code provided.  

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC try:
# MAGIC   airbnbDF
# MAGIC except NameError: # Looks for local table if airbnbDF not defined
# MAGIC   airbnbDF = spark.table("airbnb")
# MAGIC 
# MAGIC v = ",\n".join(map(lambda row: "[{}, {}, {}]".format(row[0], row[1], row[2]), airbnbDF.select(col("latitude"),col("longitude"),col("price")/600).collect()))
# MAGIC displayHTML("""
# MAGIC <html>
# MAGIC <head>
# MAGIC  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
# MAGIC    integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
# MAGIC    crossorigin=""/>
# MAGIC  <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
# MAGIC    integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
# MAGIC    crossorigin=""></script>
# MAGIC  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
# MAGIC </head>
# MAGIC <body>
# MAGIC     <div id="mapid" style="width:700px; height:500px"></div>
# MAGIC   <script>
# MAGIC   var mymap = L.map('mapid').setView([37.7587,-122.4486], 12);
# MAGIC   var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
# MAGIC     attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
# MAGIC }).addTo(mymap);
# MAGIC   var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
# MAGIC   </script>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Featurization
# MAGIC 
# MAGIC Featurize the dataset by building a pipeline that accomplishes the following:<br><br>
# MAGIC 
# MAGIC 1. Indexes all categorical variables
# MAGIC 2. One-hot encodes the indexed values
# MAGIC 3. Combines all features into a `features` column
# MAGIC 
# MAGIC Finally, perform a train/test split.

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Indexing Categorical Variables
# MAGIC 
# MAGIC Create the following columns by indexing their respective categorical feature:<br><br>
# MAGIC 
# MAGIC * `iNeighbourhood`
# MAGIC * `iRoomType`
# MAGIC * `iZipCode`
# MAGIC * `iPropertyType`
# MAGIC * `iBedType`

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
from pyspark.ml.feature import StringIndexer

iNeighbourhood = StringIndexer(inputCol="neighbourhood_cleansed", outputCol="cat_neighborhood", handleInvalid="skip")
iRoomType = StringIndexer(inputCol="room_type", outputCol="cat_room_type", handleInvalid="skip")
iZipCode = <FILL_IN>
iPropertyType = <FILL_IN>
iBedType= <FILL_IN>

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, iNeighbourhood.getOutputCol() == "cat_neighborhood")
dbTest("ML1-P-09-09-02", True, iRoomType.getOutputCol() == "cat_room_type")
dbTest("ML1-P-09-09-03", True, iZipCode.getOutputCol() == "cat_zipcode")
dbTest("ML1-P-09-09-04", True, iPropertyType.getOutputCol() == "cat_property_type")
dbTest("ML1-P-09-09-05", True, iBedType.getOutputCol() == "cat_bed_type")

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) OneHotEncoder
# MAGIC 
# MAGIC One-hot encode all previously indexed categorical features. Call the output colums `vec_neighborhood`, `vec_room_type`, `vec_zipcode`, `vec_property_type` and `vec_bed_type`, respectively.

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
from pyspark.ml.feature import OneHotEncoderEstimator

oneHotEnc = <FILL_IN>

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, set(oneHotEnc.getInputCols()) == set(["cat_neighborhood", "cat_room_type", "cat_zipcode", "cat_property_type", "cat_bed_type"]))
dbTest("ML1-P-09-09-02", True, set(oneHotEnc.getOutputCols()) == set(["vec_neighborhood", "vec_room_type", "vec_zipcode", "vec_property_type", "vec_bed_type"]))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train/Test Split
# MAGIC 
# MAGIC Perform a train/test split creating `testDF` and `trainDF`.  Keep 80% for the training set and put aside 20% of our data for the test set.

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
seed = 42
(testDF, trainDF) = airbnbDF.<FILL_IN>

print(testDF.count(), trainDF.count())

# COMMAND ----------

# TEST - Run this cell to test your solution
_traincount = trainDF.count()
_testcount = testDF.count()

dbTest("ML1-P-09-09-01", True, _traincount > 3750 and _traincount < 3850)
dbTest("ML1-P-09-09-01", True, _testcount > 900 and _testcount < 1000)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pipeline
# MAGIC 
# MAGIC Add a `VectorAssembler` and wrap the stages created above in a pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC Use the following columns as our features.

# COMMAND ----------

featureCols = [
 "host_total_listings_count",
 "accommodates",
 "bathrooms",
 "bedrooms",
 "beds",
 "minimum_nights",
 "number_of_reviews",
 "review_scores_rating",
 "review_scores_accuracy",
 "review_scores_cleanliness",
 "review_scores_checkin",
 "review_scores_communication",
 "review_scores_location",
 "review_scores_value",
 "vec_neighborhood", 
 "vec_room_type", 
 "vec_zipcode", 
 "vec_property_type", 
 "vec_bed_type"
]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Set the input columns of the `VectorAssembler` to `featureCols`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="https://spark.apache.org/docs/latest/ml-features.html#vectorassembler" target="_blank">VectorAssembler docs for details.</a>

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler( < FILL_IN > )

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, set(assembler.getInputCols()) == set(featureCols))
dbTest("ML1-P-09-09-02", True, assembler.getOutputCol() == "features")

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Put this all together in a pipeline.
# MAGIC 
# MAGIC Set `iNeighbourhood`, `iRoomType`, `iZipCode`, `iPropertyType`, `iBedType`, `oneHotEnc`, and `assembler` as the pipeline stages.  Fit the pipeline to `trainDF` and transform it, saving the result to `trainTransformedDF`

# COMMAND ----------

# TODO: Replace <FILL_IN> with appropriate code
from pyspark.ml import Pipeline

lrPipeline = < FILL_IN >
trainTransformedDF = < FILL_IN >

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", 7, len(featurizationPipeline.getStages()))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Model Training
# MAGIC 
# MAGIC Train and evaluate a baseline model.  Then, train a linear regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train a Baseline Model
# MAGIC 
# MAGIC Fit `featurizationPipeline` and store the saved model in `featurizationPipelineModel`.  Then, transform `trainDF` and `testDF` using the fit pipeline and save them to `trainTransformedDF` and `testTransformedDF`. 

# COMMAND ----------

# TODO
featurizationPipelineModel = FILL_IN

trainTransformedDF = FILL_IN
testTransformedDF = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, "features" in trainTransformedDF.columns and "features" in testTransformedDF.columns)
dbTest("ML1-P-09-09-01", True, "vec_bed_type" in trainTransformedDF.columns and "vec_bed_type" in testTransformedDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Use the average of `price` for your baseline model.  Save that value to `baselinePrediction`

# COMMAND ----------

# TODO
baselinePrediction = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, baselinePrediction > 218 and baselinePrediction < 225)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate the Baseline Model
# MAGIC 
# MAGIC Evaluate the performance of the baseline model.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `RegressionEvaluator` object named `evaluator` with `price` as the label, `prediction` as the prediction column, and `mse` as the metric.

# COMMAND ----------

# TODO
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.evaluation import RegressionEvaluator

dbTest("ML1-P-09-09-01", True, type(evaluator) == type(RegressionEvaluator()))
dbTest("ML1-P-09-09-01", True, evaluator.getLabelCol() == "price")
dbTest("ML1-P-09-09-01", True, evaluator.getPredictionCol() == "prediction")
dbTest("ML1-P-09-09-01", True, evaluator.getMetricName() == "mse")

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Create the DataFrame `baselinePredictionDF` that contains the transformed test data `testTransformedDF` with a new column `prediction` that has the same baseline prediction for all observations in the dataset.

# COMMAND ----------

# TODO
baselinePredictionDF = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, "prediction" in baselinePredictionDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate the results by running the cell below.

# COMMAND ----------

metricName = evaluator.getMetricName()
metricVal = evaluator.evaluate(baselinePredictionDF)

print("{}: {}".format(metricName, metricVal))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train a Linear Regression Model
# MAGIC 
# MAGIC Create a `LinearRegression` model object `lr` and add it to the pipeline, which combines `featurizationPipeline` and `lr`.  This new pipeline should be called `pipelineFull`.  Save the trained pipeline to `pipelineFullModel`
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** A pipeline can itself be part of a pipeline, so this pipeline should have two stages: our initial pipeline `featurizationPipeline` and the new estimator `lr`

# COMMAND ----------

# TODO
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

lr = FILL_IN
pipelineFull = FILL_IN

pipelineFullModel = pipelineFull.fit(trainDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.regression import LinearRegression

dbTest("ML1-P-09-09-01", True, type(lr) == type(LinearRegression()))
dbTest("ML1-P-09-09-01", True, lr.getLabelCol() == "price")
dbTest("ML1-P-09-09-01", True, lr.getFeaturesCol() == "features")
dbTest("ML1-P-09-09-01", True, len(pipelineFull.getStages()) == 2 or len(pipelineFull.getStages()) == 8)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate the performance of linear regression over the baseline model.

# COMMAND ----------

metricName = evaluator.getMetricName()
metricVal = evaluator.evaluate(pipelineFullModel.transform(testDF))

print("{}: {}".format(metricName, metricVal))

# COMMAND ----------

# MAGIC %md
# MAGIC How much did our model improve?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Hyperparameter Tuning
# MAGIC 
# MAGIC Use 3-fold cross-validation to tune the hyperparameters of the linear regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the parameters for `LinearRegression()`.

# COMMAND ----------

print(lr.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC Build a grid of parameters for cross-validation.  The grid should consist of the following:
# MAGIC 
# MAGIC | Parameter          | Values      |
# MAGIC |--------------------|-------------|
# MAGIC | lr.elasticNetParam | 0.0, 1.0    |
# MAGIC | lr.maxIter         | 1, 10, 100  |
# MAGIC | lr.fitIntercept    | True, False |

# COMMAND ----------

# TODO
from pyspark.ml.tuning import ParamGridBuilder

paramGrid = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-09-09-01", True, type(paramGrid) == list)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Build the cross-validator.

# COMMAND ----------

# TODO
from pyspark.ml.tuning import CrossValidator

cv = FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.tuning import CrossValidator

dbTest("ML1-P-09-09-01", True, type(cv) == type(CrossValidator()))
dbTest("ML1-P-09-09-01", True, cv.getEstimator() == pipelineFull)
dbTest("ML1-P-09-09-01", True, cv.getEstimatorParamMaps() == paramGrid)
dbTest("ML1-P-09-09-01", True, cv.getEvaluator() == evaluator)
dbTest("ML1-P-09-09-01", True, cv.getNumFolds() == 3)


print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Fit the cross-validator to the training data.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This could take a while to train.

# COMMAND ----------

cvModel = cv.fit(trainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Examine the results.

# COMMAND ----------

for params, score in zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics):
  print("".join([param.name+"\t"+str(params[param])+"\t" for param in params]))
  print("\tScore: {}".format(score))

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate on training and test data

# COMMAND ----------

evaluator.evaluate(cvModel.bestModel.transform(trainDF)) # Training data

# COMMAND ----------

evaluator.evaluate(cvModel.bestModel.transform(testDF)) # Test data

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT Next Steps<br>
# MAGIC 
# MAGIC * Please complete the <a href="https://www.surveymonkey.com/r/5RSX9FZ" target="_blank">short feedback survey</a>.  Your input is extremely important and shapes future course development.
# MAGIC * Congratulations, you have completed Introduction to Data Science and Machine Learning!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>