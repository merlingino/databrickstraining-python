# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning Workflows
# MAGIC 
# MAGIC Machine learning practitioners generally follow an iterative workflow.  This lesson walks through that workflow at a high level before exploring train/test splits, a baseline model, and evaluation.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Define the data analytics development cycle
# MAGIC * Motivate and perform a split between training and test data
# MAGIC * Train a baseline model
# MAGIC * Evaluate a baseline model's performance and improve it

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/qimsc8jn4a?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/qimsc8jn4a?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Development Cycle
# MAGIC 
# MAGIC Data scientists follow an iterative workflow that keeps their work closely aligned to both business problems and their data.  This cycle begins with a thorough understanding of the business problem and the data itself, a process called _exploratory data analysis_.  Once the motivating business question and data are understood, the next step is preparing the data for modeling.  This includes removing or imputing missing values and outliers as well as creating features to train the model on.  The majority of a data scientist's work is spent in these earlier steps.
# MAGIC 
# MAGIC After preparing the features in a way that the model can benefit from, the modeling stage uses those features to determine the best way to represent the data.  The various models are then evaluated and this whole process is repeated until the best solution is developed and deployed into production.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/CRISP-DM.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC The above model addresses the high-level development cycle of data products.  This lesson addresses how to implement this at more practical level.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://en.wikipedia.org/wiki/Cross-industry_standard_process_for_data_mining" target="_blank">See the Cross-Industry Standard Process for Data Mining</a> for details on the method above.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Train/Test Split
# MAGIC 
# MAGIC To implement the development cycle detailed above, data scientists first divide their data randomly into two subsets.  This allows for the evaluation of the model on unseen data.<br><br>
# MAGIC 
# MAGIC 1. The **training set** is used to train the model on
# MAGIC 2. The **test set** is used to test how well the model performs on unseen data
# MAGIC 
# MAGIC This split avoids the memorization of data, known as **overfitting**.  Overfitting occurs when our model learns patterns caused by random chance rather than true signal.  By evaluating our model's performance on unseen data, we can minimize overfitting.
# MAGIC 
# MAGIC Splitting training and test data should be done so that the amount of data in the test set is a good sample of the overall data.  **A split of 80% of your data in the training set and 20% in the test set is a good place to start.**
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/train-test-split.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
)

display(bostonDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Split the dataset into two DataFrames:<br><br>
# MAGIC 
# MAGIC 1. `trainDF`: our training data
# MAGIC 2. `testDF`: our test data
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using a seed ensures that the random split we conduct will be the same split if we rerun the code again.  Reproducible experiments are a hallmark of good science.<br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Conventions using other machine learning tools often entail creating 4 objects: `X_train`, `y_train`, `X_test`, and `y_test` where your features `X` are separate from your label `y`.  Since Spark is distributed, the Spark convention keeps the features and labels together when the split is performed.

# COMMAND ----------

trainDF, testDF = bostonDF.randomSplit([0.8, 0.2], seed=42)

print("We have {} training examples and {} test examples.".format(trainDF.count(), testDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Model
# MAGIC 
# MAGIC A **baseline model** offers an educated best guess to improve upon as different models are trained and evaluated.  It represents the simplest model we can create.  This is generally approached as the center of the data.  In the case of regression, this could involve predicting the average of the outcome regardless of the features it sees.  In the case of classification, the center of the data is the mode, or the most common class.  
# MAGIC 
# MAGIC A baseline model could also be a random value or a preexisting model.  Through each new model, we can track improvements with respect to this baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a baseline model by calculating the average housing value in the training dataset.

# COMMAND ----------

from pyspark.sql.functions import avg

trainAvg = trainDF.select(avg("medv")).first()[0]

print("Average home value: {}".format(trainAvg))

# COMMAND ----------

# MAGIC %md
# MAGIC Take the average calculated on the training dataset and append it as the column `prediction` on the test dataset.

# COMMAND ----------

from pyspark.sql.functions import lit

testPredictionDF = testDF.withColumn("prediction", lit(trainAvg))

display(testPredictionDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Evaluation and Improvement
# MAGIC 
# MAGIC Evaluation offers a way of measuring how well predictions match the observed data.  In other words, an evaluation metric measures how closely predicted responses are to the true response.
# MAGIC 
# MAGIC There are a number of different evaluation metrics.  The most common evaluation metric in regression tasks is **mean squared error (MSE)**.  This is calculated by subtracting each predicted response from the corresponding true response and squaring the result.  This assures that the result is always positive.  The lower the MSE, the better the model is performing.  Technically:
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/mse.png" style="height: 100px; margin: 20px"/></div>
# MAGIC 
# MAGIC Since we care about how our model performs on unseen data, we are more concerned about the test error, or the MSE calculated on the unseen data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Define the evaluator with the prediction column, label column, and MSE metric.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll explore various model parameters in later lessons.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="medv", metricName="mse")

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate `testPredictionDF` using the `.evaluator()` method.

# COMMAND ----------

testError = evaluator.evaluate(testPredictionDF)

print("Error on the test set for the baseline model: {}".format(testError))

# COMMAND ----------

# MAGIC %md
# MAGIC This score indicates that the average squared distance between the true home value and the prediction of the baseline is about 79.  Taking the square root of that number gives us the error in the units of the quantity being estimated.  In other words, taking the square root of 79 gives us an average error of about $8,890.  That's not great, but it's also not too bad for a naive approach.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Train/Test Split and Baseline Model
# MAGIC 
# MAGIC Do a train/test split on a Dataset, create a baseline model, and evaluate the result.  Optionally, try to beat this baseline model by training a linear regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Train/Test Split
# MAGIC 
# MAGIC Import the bike sharing dataset and take a look at what's in it.  This dataset contains number of bikes rented (`cnt`) by season, year, month, and hour and for a number of weather conditions.

# COMMAND ----------

bikeDF = (spark
  .read
  .option("header", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bikeSharing/data-001/hour.csv")
  .drop("instant", "dteday", "casual", "registered", "holiday", "weekday") # Drop unnecessary features
)

display(bikeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split.  Put 70% of the data into `trainBikeDF` and 30% into `testBikeDF`.  Use a seed of `42` so you have the same split every time you perform the operation.

# COMMAND ----------

# TODO
trainBikeDF, testBikeDF = bikeDF.randomSplit([0.7, 0.3], seed=42)

# COMMAND ----------

# TEST - Run this cell to test your solution
_traincount = trainBikeDF.count()
_testcount = testBikeDF.count()

dbTest("ML1-P-03-01-01", True, _traincount < 13000 and _traincount > 12000)
dbTest("ML1-P-03-01-02", True, _testcount < 5500 and _testcount > 4800)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a Baseline Model
# MAGIC 
# MAGIC Calculate the average of the column `cnt` and save it to the variable `trainCnt`.  Then create a new DataFrame `bikeTestPredictionDF` that appends a new column `prediction` that's the value of `trainCnt`.

# COMMAND ----------

from pyspark.sql.functions import col, avg, lit

# COMMAND ----------

from pyspark.sql.functions import col, avg

# TODO
avgTrainCnt = trainBikeDF.select(avg("cnt")).first()[0]
bikeTestPredictionDF = trainBikeDF.withColumn("prediction", lit(avgTrainCnt))

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-03-02-01", True, avgTrainCnt < 195 and avgTrainCnt > 180)
dbTest("ML1-P-03-02-02", True, "prediction" in bikeTestPredictionDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3: Evaluate the Result
# MAGIC 
# MAGIC Evaluate the result using `mse` as the error metric.  Save the result to `testError`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Your baseline prediction will not be very accurate.  Be sure to take the square root of the MSE to return the results to the proper units (that is, bike counts).

# COMMAND ----------

# TODO
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="cnt", metricName="mse")
testError = evaluator.evaluate(bikeTestPredictionDF)

print("Error on the test set for the baseline model: {}".format(testError))

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-03-03-01", True, testError > 33000 and testError < 35000)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 (Optional): Beat the Baseline
# MAGIC 
# MAGIC Use a linear regression model (explored in the previous lesson) to beat the baseline model score.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What does a data scientist's workflow look like?  
# MAGIC **Answer:** Data scientists employ an iterative workflow that includes the following steps:
# MAGIC 0. *Business and Data Understanding:* ensures a rigorous understanding of both the business problem and the available data
# MAGIC 0. *Data Preparation:* involves cleaning data so that it can be fed into algorithms and create new features
# MAGIC 0. *Modeling:* entails training many models and many combinations of parameters for a given model
# MAGIC 0. *Evaluation:* compares model performance and chooses the best option
# MAGIC 0. *Deployment:* launches a model into production where it's used to inform business decision-making<br>
# MAGIC 
# MAGIC **Question:** Why should I divide all my data into two subsets?  
# MAGIC **Answer:** It's important to gauge how a model performs on unseen data.  Without this check, the model will "overfit," where it memorizes both the signal and the noise in the dataset rather than just learning the true signal.  In practice, many models are trained on the training dataset and tested on the test dataset.  Before launching into production, the final model is often retrained on all of the data since the more data a model sees, the better it generally performs.
# MAGIC 
# MAGIC **Question:** What does a baseline model do?  
# MAGIC **Answer:** Since training machine learning models is an iterative process, using a naive baseline model offers a reference point for what a basic solution would offer.  Baseline models are sometimes arbitrary (such as a using a random prediction), but that reference point grounds future hypotheses.
# MAGIC 
# MAGIC **Question:** How do I evaluate the performance of a regression model?  
# MAGIC **Answer:** There are a number of ways of evaluating regression models.  The most common way of evaluating regression models is using Mean Squared Error (MSE).  This calculates the average squared distance between the predicted value and the true value. By squaring the error, we will always get a positive number so this evaluation metric does not care if the prediction is above or below the true value.  There are many alternatives, including Root Mean Squared Error (RMSE).  RMSE is a helpful metric because, by taking the square root of the MSE, the error has the same units as the dependent variable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Featurization]($./05-Featurization ).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>