# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Regression Modeling
# MAGIC 
# MAGIC Linear regression is the most commonly employed machine learning model since it is highly interpretable and well studied.  This is often the first pass for data scientists modeling continuous variables.  This lesson trains simple and multivariate regression models and interprets the results.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Motivate the use of linear regression
# MAGIC * Train a simple regression model
# MAGIC * Interpret regression models
# MAGIC * Train a multivariate regression model

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/xfemo2c5fn?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xfemo2c5fn?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Lines through Data
# MAGIC 
# MAGIC Take the example of Boston housing data where we have median value for a number of neighborhoods and variables such as the number of rooms, per capita crime, and economic status of residents.  We might have a number of questions about this data including:<br><br>
# MAGIC 
# MAGIC 1. *Is there a relationship* between our features and median home value?
# MAGIC 2. If there is a relationship, *how strong is that relationship?*
# MAGIC 3. *Which of the features* affect median home value?
# MAGIC 4. *How accurately can we estimate* the effect of each feature on home value?
# MAGIC 5. *How accurately can we predict* on unseen data?
# MAGIC 6. Is the relationship between our features and home value *linear*?
# MAGIC 7. Are there *interaction effects* (e.g. value goes up when an area is not industrial and has more rooms on average) between the features?
# MAGIC 
# MAGIC Generally speaking, machine learning models either allow us to infer something about our data or create accurate predictions.  **There is a trade-off between model accuracy and interpretability.**  Linear regression is a highly interpretable model, allowing us to infer the answers to the questions above.  The predictive power of this model is somewhat limited, however, so if we're concerned about how our model will work on unseen data, we might choose a different model.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/rm-vs-mdv.png" style="height: 600px; margin: 20px"/></div>
# MAGIC 
# MAGIC At a high level, **linear regression can be thought of as lines put through data.**  The line plotted above uses a linear regression model to create a best guess for the relationship between average number of rooms in a home and home value.  

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Simple Linear Regression
# MAGIC 
# MAGIC Simple linear regression looks to predict a response `Y` using a single input variable `X`.  In the case of the image above, we're predicting median home value, or `Y`, based on the average number of rooms.  More technically, linear regression is estimating the following equation:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`Y ≈ β<sub>0</sub> + β<sub>1</sub>X`
# MAGIC 
# MAGIC In this case, `β<sub>0</sub>` and `β<sub>1</sub>` are our **coefficients** where `β<sub>0</sub>` represents the line's intercept with the Y axis and `β<sub>1</sub>` represents the number we multiply by X in order to attain a prediction.  **A simple linear regression model will try to fit our data a closely as possible by estimating these coefficients,** putting a line through the data.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the case of inferential statistics where we're interested in learning about the relationship between our input features and outputs, it's common to skip the train/test split step, as you'll see in this lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
  .drop("_c0")
)

display(bostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a column `features` that has a single input variable `rm` by using `VectorAssembler`

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

featureCol = ["rm"]
assembler = VectorAssembler(inputCols=featureCol, outputCol="features")

bostonFeaturizedDF = assembler.transform(bostonDF)

display(bostonFeaturizedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Fit a linear regression model.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectorassembler#pyspark.ml.regression.LinearRegression" target="_blank">LinearRegression</a> documentation for more details.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="medv")

lrModel = lr.fit(bostonFeaturizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Interpretation
# MAGIC 
# MAGIC Interpreting a linear model entails answering a number of questions:<br><br>
# MAGIC 
# MAGIC 1. What did the model estimate my coefficients to be?
# MAGIC 2. Are my coefficients statistically significant?
# MAGIC 3. How accurate was my model?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Recalling that our model looks like `Y ≈ β<sub>0</sub> + β<sub>1</sub>X`, take a look at the model.

# COMMAND ----------

print("β0 (intercept): {}".format(lrModel.intercept))
print("β1 (coefficient for rm): {}".format(*lrModel.coefficients))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC For a 5 bedroom home, our model would predict `-35.7 + (9.1 * 5)` or `$18,900`.  That's not too bad.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The intercept of `-34.7` doesn't make a lot of sense on its own since this would imply that a studio apartment would be worth negative dollars.  Also, we don't have any 1 or 2 bedroom homes in our dataset, so the model will perform poorly on data in this range.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC In order to determine whether our coefficients are statistically significant, we need to quantify the likelihood of seeing the association by chance.  One way of doing this is using a p-value.  As a general rule of thumb, a p-value of under .05 indicates statistical significance in that there is less than a 1 in 20 chance of seeing the correlation by mere chance.
# MAGIC 
# MAGIC Do this using the `summary` attribute of `lrModel`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The t-statistic can be used instead of p-values.  <a href="https://en.wikipedia.org/wiki/P-value" target="_blank">Read more about p-values here.</a>

# COMMAND ----------

summary = lrModel.summary

summary.pValues

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC These small p-values indicate that it is highly unlikely to see the correlation of the number of rooms to housing price by chance.  The first value in the list is the p-value for the `rm` feature and the second is that for the intercept.
# MAGIC 
# MAGIC Finally, we need a way to quantify how accurate our model is.  **R<sup>2</sup> is a measure of the proportion of variance in the dataset explained by the model.**  With R<sup>2</sup>, a higher number is better.

# COMMAND ----------

summary.r2

# COMMAND ----------

# MAGIC %md
# MAGIC This indicates that 48% of the variability in home value can be explained using `rm` and the intercept.  While this isn't too high, it's not too bad considering that we're training a model using only one variable.

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, take a look at the `summary` attribute of `lrModel` so see other ways of summarizing model performance.

# COMMAND ----------

[attr for attr in dir(summary) if attr[0] != "_"]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Multivariate Regression
# MAGIC 
# MAGIC While simple linear regression involves just a single input feature, multivariate regression takes an arbitrary number of input features.  The same principles apply that we explored in the simple regression example.  The equation for multivariate regression looks like the following where each feature `p` has its own coefficient:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`Y ≈ β<sub>0</sub> + β<sub>1</sub>X<sub>1</sub> + β<sub>2</sub>X<sub>2</sub> + ... + β<sub>p</sub>X<sub>p</sub>`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Our ability to visually explain how our model is performing becomes more limited as our number of features go up since we can only intuitively visualize data in two, possibly three dimensions.  With multivariate regression, we're therefore still putting lines through data, but this is happening in a higher dimensional space.

# COMMAND ----------

# MAGIC %md
# MAGIC Train a multivariate regression model using `rm`, `crim`, and `lstat` as the input features.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

featureCols = ["rm", "crim", "lstat"]
assemblerMultivariate = VectorAssembler(inputCols=featureCols, outputCol="features")

bostonFeaturizedMultivariateDF = assemblerMultivariate.transform(bostonDF)

display(bostonFeaturizedMultivariateDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Train the model.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lrMultivariate = (LinearRegression()
  .setLabelCol("medv")
  .setFeaturesCol("features")
)

lrModelMultivariate = lrMultivariate.fit(bostonFeaturizedMultivariateDF)

summaryMultivariate = lrModelMultivariate.summary

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Take a look at the coefficients and R<sup>2</sup> score.

# COMMAND ----------

print("β0 (intercept): {}".format(lrModelMultivariate.intercept))
for i, (col, coef) in enumerate(zip(featureCols, lrModelMultivariate.coefficients)):
  print("β{} (coefficient for {}): {}".format(i+1, col, coef))
  
print("\nR2 score: {}".format(lrModelMultivariate.summary.r2))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Our R<sup>2</sup> score improved from 48% to 64%, indicating that our new model can explain more of the variance in the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Improve on our Model
# MAGIC 
# MAGIC Improve on the model trained above by adding features and interpreting the results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare the Features for a New Model
# MAGIC 
# MAGIC Prepare a new column `allFeatures` for a new model that uses all of the features in `bostonDF` except for the label `medv`.  Create the following variables:<br><br>
# MAGIC 
# MAGIC 1. `allFeatures`: a list of all the column names
# MAGIC 2. `assemblerAllFeatures`: A `VectorAssembler` that uses `allFeatures` to create the output column `allFeatures`
# MAGIC 3. `bostonFeaturizedAllFeaturesDF`: The transformed `bostonDF`

# COMMAND ----------

allFeatures = [f for f in bostonDF.columns if f != "medv"]
print(allFeatures)

# COMMAND ----------

# TODO
allFeatures = [f for f in bostonDF.columns if f != "medv"]
assemblerAllFeatures = VectorAssembler(inputCols=allFeatures, outputCol="allFeatures")

bostonFeaturizedAllFeaturesDF = assemblerAllFeatures.transform(bostonDF)
display(bostonFeaturizedAllFeaturesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.feature import VectorAssembler

_features = ['crim',
  'zn',
  'indus',
  'chas',
  'nox',
  'rm',
  'age',
  'dis',
  'rad',
  'tax',
  'ptratio',
  'black',
  'lstat'
]

dbTest("ML1-P-06-01-01", _features, allFeatures)
dbTest("ML1-P-06-01-02", True, type(assemblerAllFeatures) == type(VectorAssembler()))
dbTest("ML1-P-06-01-03", True, assemblerAllFeatures.getOutputCol() == 'allFeatures')
dbTest("ML1-P-06-01-04", True, "allFeatures" in bostonFeaturizedAllFeaturesDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Train the Model
# MAGIC 
# MAGIC Create a linear regression model `lrAllFeatures`.  Save the trained model to lrModelAllFeatures.

# COMMAND ----------

# TODO
from pyspark.ml.regression import LinearRegression


lrAllFeatures = (LinearRegression()
                 .setLabelCol("medv")
                 .setFeaturesCol("allFeatures"))

lrModelAllFeatures = lrAllFeatures.fit(bostonFeaturizedAllFeaturesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.regression import LinearRegression

dbTest("ML1-P-06-02-01", True, type(lrAllFeatures) == type(LinearRegression()))
dbTest("ML1-P-06-02-02", True, lrAllFeatures.getLabelCol() == 'medv')
dbTest("ML1-P-06-02-03", True, lrAllFeatures.getFeaturesCol() == 'allFeatures')
dbTest("ML1-P-06-02-04", True, "LinearRegressionModel" in str(type(lrModelAllFeatures)))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Interpret the Coefficients and Variance Explained
# MAGIC 
# MAGIC Take a look at the coefficients and variance explained.  What do these mean?

# COMMAND ----------

# TODO
summary = lrModelAllFeatures.summary

# Print R Squared
print('R Squared (Percent of variability expalined by the model):',summary.r2)

print('*'*20)

# print Coefficients
print('Feature Coeficcients')
print("β0 (intercept): {}".format(lrModelAllFeatures.intercept))
for i, (col, coef) in enumerate(zip(allFeatures, lrModelAllFeatures.coefficients)):
  print("β{} (coefficient for {}): {}".format(i+1, col, coef))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Interpret the Statistical Significance of the Coefficients
# MAGIC 
# MAGIC Print out the p-values associated with each coefficient and the intercept.  Which were statistically significant?

# COMMAND ----------

# TODO

# Model P Values (< 0.05, indicates there is a less than that chance of seering the correlation by chance)
print('P Values (< 0.05, indicates there is a less than that chance of seeing the correlation by chance)')
for i, (col, pval) in enumerate(zip(allFeatures, summary.pValues)):
  if pval < 0.05:
    print("{} P Value {}".format(col,pval))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What are the pros and cons of linear regression?  
# MAGIC **Answer:** Linear regression is an excellent tool for first getting to know your data, bridging the gap between data exploration and prediction.  It is a highly interpretable model that offers a sense for which of our features are statistically significant and how much they influence the final model.  There are two main drawbacks to this model, however.  The first is that it does not have strong predictive power.  Other models such as random forests or neural networks are able to find more complex relationships that linear regression struggles to model.  The second limitation is that it assumes a linear relationship between features and outcomes.  This assumption often works well for a first model however more precise models demand a way of understanding more complex relationships.
# MAGIC 
# MAGIC **Question:** When should I use simple regression and how does it work?  
# MAGIC **Answer:** In practice, simple regression is not used often since data scientists normally model many features rather than just one.  It works by estimating the line that best fits the data.  Underneath the hood, the simple regression algorithm is trying to minimize the distance between the line and the observed data.  In practice, it will never reduce this distance to zero, but the algorithm will have found the best fit when it can not reduce the distance any further.
# MAGIC 
# MAGIC **Question:** What am I looking for when I interpret the a model and how do I see this in the results?  
# MAGIC **Answer:** Interpreting a model looks at a number of factors including (but not limited to):
# MAGIC 0. *Coefficients:* what did the model estimate the coefficients to be?
# MAGIC 0. *Statistical Significance* were each of the coefficients statistically significant or should some be removed?
# MAGIC 0. *Accuracy* how well did my model explain the signal in the dataset?<br>
# MAGIC 
# MAGIC **Question:** Does Spark standardize my data?  
# MAGIC **Answer:** Yes.  Spark standardizes each feature by default so the user does not need to take this pre-processing step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Classification]($./07-Classification ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on machine learning using Spark?  
# MAGIC **A:** Check out <a href="https://spark.apache.org/docs/latest/ml-guide.html" target="_blank">the Apache Spark website for more information</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>