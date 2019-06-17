# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC 
# MAGIC # Working with Time Windows
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use sliding windows to aggregate over chunks of data rather than all data
# MAGIC * Apply watermarking to throw away stale old data that you do not have space to keep
# MAGIC * Plot live graphs using `display`
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Scientists, Software Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses DataFrames, ETL-Parts 1, 2 and 3 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/uiwie12hng?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/uiwie12hng?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Streaming Aggregations</h2>
# MAGIC 
# MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
# MAGIC 
# MAGIC Some examples include 
# MAGIC * Aggregating errors in data from IoT devices by type 
# MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country. 
# MAGIC * Doing behavior analysis on instant messages via hash tags.
# MAGIC 
# MAGIC However, in the case of streams, you generally don't want to run aggregations over the entire dataset. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### What problems might you encounter if you aggregate over a stream's entire dataset?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <script type="text/javascript">
# MAGIC   window.onload = function() {
# MAGIC     var allHints = document.getElementsByClassName("hint-8723");
# MAGIC     var answer = document.getElementById("answer-8723");
# MAGIC     var totalHints = allHints.length;
# MAGIC     var nextHint = 0;
# MAGIC     var hasAnswer = (answer != null);
# MAGIC     var items = new Array();
# MAGIC     var answerLabel = "Click here for the answer";
# MAGIC     for (var i = 0; i < totalHints; i++) {
# MAGIC       var elem = allHints[i];
# MAGIC       var label = "";
# MAGIC       if ((i + 1) == totalHints)
# MAGIC         label = answerLabel;
# MAGIC       else
# MAGIC         label = "Click here for the next hint";
# MAGIC       items.push({label: label, elem: elem});
# MAGIC     }
# MAGIC     if (hasAnswer) {
# MAGIC       items.push({label: '', elem: answer});
# MAGIC     }
# MAGIC 
# MAGIC     var button = document.getElementById("hint-button-8723");
# MAGIC     if (totalHints == 0) {
# MAGIC       button.innerHTML = answerLabel;
# MAGIC     }
# MAGIC     button.onclick = function() {
# MAGIC       items[nextHint].elem.style.display = 'block';
# MAGIC       if ((nextHint + 1) >= items.length)
# MAGIC         button.style.display = 'none';
# MAGIC       else
# MAGIC         button.innerHTML = items[nextHint].label;
# MAGIC         nextHint += 1;
# MAGIC     };
# MAGIC     button.ondblclick = function(e) {
# MAGIC       e.stopPropagation();
# MAGIC     }
# MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
# MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
# MAGIC       var elem = answerCodeBlocks[i];
# MAGIC       var parent = elem.parentNode;
# MAGIC       if (parent.name != "pre") {
# MAGIC         var newNode = document.createElement("pre");
# MAGIC         newNode.append(elem.cloneNode(true));
# MAGIC         elem.replaceWith(newNode);
# MAGIC         elem = newNode;
# MAGIC       }
# MAGIC       elem.ondblclick = function(e) {
# MAGIC         e.stopPropagation();
# MAGIC       };
# MAGIC 
# MAGIC       elem.style.marginTop = "1em";
# MAGIC     }
# MAGIC   };
# MAGIC </script>
# MAGIC 
# MAGIC <div>
# MAGIC   <button type="button" class="btn btn-light"
# MAGIC           style="margin-top: 1em"
# MAGIC           id="hint-button-8723">Click here for a hint</button>
# MAGIC </div>
# MAGIC <div id="answer-8723" style="padding-bottom: 20px; display: none">
# MAGIC   The answer:
# MAGIC   <div class="answer" style="margin-left: 1em">
# MAGIC While streams have a definitive start, there conceptually is no end to the flow of data.
# MAGIC 
# MAGIC Because there is no &quot;end&quot; to a stream, the size of the dataset grows in perpetuity.
# MAGIC 
# MAGIC This means that your cluster will eventually run out of resources.
# MAGIC 
# MAGIC Instead of aggregating over the entire dataset, you can aggregate over data grouped by windows of time (say, every 5 minutes or every hour).
# MAGIC 
# MAGIC This is referred to as windowing
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowing</h2>
# MAGIC 
# MAGIC If we were using a static DataFrame to produce an aggregate count, we could use `groupBy()` and `count()`. 
# MAGIC 
# MAGIC Instead we accumulate counts within a sliding window, answering questions like "How many records are we getting every second?"
# MAGIC 
# MAGIC The following illustration, from the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a> guide, helps us understanding how it works:

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Time vs Receipt Time</h2>
# MAGIC 
# MAGIC **Event Time** is the time at which the event occurred in the real world.
# MAGIC 
# MAGIC **Event Time** is **NOT** something maintained by the Structured Streaming framework. 
# MAGIC 
# MAGIC At best, Streams only knows about **Receipt Time** - the time a piece of data arrived in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are some examples of **Event Time**? **of Receipt Time**?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <script type="text/javascript">
# MAGIC   window.onload = function() {
# MAGIC     var allHints = document.getElementsByClassName("hint-6092");
# MAGIC     var answer = document.getElementById("answer-6092");
# MAGIC     var totalHints = allHints.length;
# MAGIC     var nextHint = 0;
# MAGIC     var hasAnswer = (answer != null);
# MAGIC     var items = new Array();
# MAGIC     var answerLabel = "Click here for the answer";
# MAGIC     for (var i = 0; i < totalHints; i++) {
# MAGIC       var elem = allHints[i];
# MAGIC       var label = "";
# MAGIC       if ((i + 1) == totalHints)
# MAGIC         label = answerLabel;
# MAGIC       else
# MAGIC         label = "Click here for the next hint";
# MAGIC       items.push({label: label, elem: elem});
# MAGIC     }
# MAGIC     if (hasAnswer) {
# MAGIC       items.push({label: '', elem: answer});
# MAGIC     }
# MAGIC 
# MAGIC     var button = document.getElementById("hint-button-6092");
# MAGIC     if (totalHints == 0) {
# MAGIC       button.innerHTML = answerLabel;
# MAGIC     }
# MAGIC     button.onclick = function() {
# MAGIC       items[nextHint].elem.style.display = 'block';
# MAGIC       if ((nextHint + 1) >= items.length)
# MAGIC         button.style.display = 'none';
# MAGIC       else
# MAGIC         button.innerHTML = items[nextHint].label;
# MAGIC         nextHint += 1;
# MAGIC     };
# MAGIC     button.ondblclick = function(e) {
# MAGIC       e.stopPropagation();
# MAGIC     }
# MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
# MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
# MAGIC       var elem = answerCodeBlocks[i];
# MAGIC       var parent = elem.parentNode;
# MAGIC       if (parent.name != "pre") {
# MAGIC         var newNode = document.createElement("pre");
# MAGIC         newNode.append(elem.cloneNode(true));
# MAGIC         elem.replaceWith(newNode);
# MAGIC         elem = newNode;
# MAGIC       }
# MAGIC       elem.ondblclick = function(e) {
# MAGIC         e.stopPropagation();
# MAGIC       };
# MAGIC 
# MAGIC       elem.style.marginTop = "1em";
# MAGIC     }
# MAGIC   };
# MAGIC </script>
# MAGIC 
# MAGIC <div>
# MAGIC   <button type="button" class="btn btn-light"
# MAGIC           style="margin-top: 1em"
# MAGIC           id="hint-button-6092">Click here for a hint</button>
# MAGIC </div>
# MAGIC <div id="answer-6092" style="padding-bottom: 20px; display: none">
# MAGIC   The answer:
# MAGIC   <div class="answer" style="margin-left: 1em">
# MAGIC #### Examples of *Event Time*:
# MAGIC * The timestamp recorded in each record of a log file
# MAGIC * The instant at which an IoT device took a measurement
# MAGIC * The moment a REST API received a request
# MAGIC 
# MAGIC #### Examples of *Receipt Time*:
# MAGIC * A timestamp added to a DataFrame the moment it was processed by Spark
# MAGIC * The timestamp extracted from an hourly log file&#x27;s file name
# MAGIC * The time at which an IoT hub received a report of a device&#x27;s measurement 
# MAGIC   - Presumably offset by some delay from when the measurement was taken
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are some of the inherent problems with using **Receipt Time**?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <script type="text/javascript">
# MAGIC   window.onload = function() {
# MAGIC     var allHints = document.getElementsByClassName("hint-2538");
# MAGIC     var answer = document.getElementById("answer-2538");
# MAGIC     var totalHints = allHints.length;
# MAGIC     var nextHint = 0;
# MAGIC     var hasAnswer = (answer != null);
# MAGIC     var items = new Array();
# MAGIC     var answerLabel = "Click here for the answer";
# MAGIC     for (var i = 0; i < totalHints; i++) {
# MAGIC       var elem = allHints[i];
# MAGIC       var label = "";
# MAGIC       if ((i + 1) == totalHints)
# MAGIC         label = answerLabel;
# MAGIC       else
# MAGIC         label = "Click here for the next hint";
# MAGIC       items.push({label: label, elem: elem});
# MAGIC     }
# MAGIC     if (hasAnswer) {
# MAGIC       items.push({label: '', elem: answer});
# MAGIC     }
# MAGIC 
# MAGIC     var button = document.getElementById("hint-button-2538");
# MAGIC     if (totalHints == 0) {
# MAGIC       button.innerHTML = answerLabel;
# MAGIC     }
# MAGIC     button.onclick = function() {
# MAGIC       items[nextHint].elem.style.display = 'block';
# MAGIC       if ((nextHint + 1) >= items.length)
# MAGIC         button.style.display = 'none';
# MAGIC       else
# MAGIC         button.innerHTML = items[nextHint].label;
# MAGIC         nextHint += 1;
# MAGIC     };
# MAGIC     button.ondblclick = function(e) {
# MAGIC       e.stopPropagation();
# MAGIC     }
# MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
# MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
# MAGIC       var elem = answerCodeBlocks[i];
# MAGIC       var parent = elem.parentNode;
# MAGIC       if (parent.name != "pre") {
# MAGIC         var newNode = document.createElement("pre");
# MAGIC         newNode.append(elem.cloneNode(true));
# MAGIC         elem.replaceWith(newNode);
# MAGIC         elem = newNode;
# MAGIC       }
# MAGIC       elem.ondblclick = function(e) {
# MAGIC         e.stopPropagation();
# MAGIC       };
# MAGIC 
# MAGIC       elem.style.marginTop = "1em";
# MAGIC     }
# MAGIC   };
# MAGIC </script>
# MAGIC 
# MAGIC <div>
# MAGIC   <button type="button" class="btn btn-light"
# MAGIC           style="margin-top: 1em"
# MAGIC           id="hint-button-2538">Click here for a hint</button>
# MAGIC </div>
# MAGIC <div id="answer-2538" style="padding-bottom: 20px; display: none">
# MAGIC   The answer:
# MAGIC   <div class="answer" style="margin-left: 1em">
# MAGIC The main problem with using **Receipt Time** is going to be with accuracy. For example:
# MAGIC 
# MAGIC * The time between when an IoT device takes a measurement vs when it is reported can be off by several minutes. 
# MAGIC   - This could have significant ramifications to security and health devices, for example
# MAGIC * The timestamp embedded in an hourly log file can be off by up to one hour making correlations to other events extremely difficult
# MAGIC * The timestamp added by Spark as part of a DataFrame transformation can be off by hours to weeks to months depending on when the event occurred and when the job ran
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### When might it be OK to use **Receipt Time** instead of **Event Time**?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <script type="text/javascript">
# MAGIC   window.onload = function() {
# MAGIC     var allHints = document.getElementsByClassName("hint-9945");
# MAGIC     var answer = document.getElementById("answer-9945");
# MAGIC     var totalHints = allHints.length;
# MAGIC     var nextHint = 0;
# MAGIC     var hasAnswer = (answer != null);
# MAGIC     var items = new Array();
# MAGIC     var answerLabel = "Click here for the answer";
# MAGIC     for (var i = 0; i < totalHints; i++) {
# MAGIC       var elem = allHints[i];
# MAGIC       var label = "";
# MAGIC       if ((i + 1) == totalHints)
# MAGIC         label = answerLabel;
# MAGIC       else
# MAGIC         label = "Click here for the next hint";
# MAGIC       items.push({label: label, elem: elem});
# MAGIC     }
# MAGIC     if (hasAnswer) {
# MAGIC       items.push({label: '', elem: answer});
# MAGIC     }
# MAGIC 
# MAGIC     var button = document.getElementById("hint-button-9945");
# MAGIC     if (totalHints == 0) {
# MAGIC       button.innerHTML = answerLabel;
# MAGIC     }
# MAGIC     button.onclick = function() {
# MAGIC       items[nextHint].elem.style.display = 'block';
# MAGIC       if ((nextHint + 1) >= items.length)
# MAGIC         button.style.display = 'none';
# MAGIC       else
# MAGIC         button.innerHTML = items[nextHint].label;
# MAGIC         nextHint += 1;
# MAGIC     };
# MAGIC     button.ondblclick = function(e) {
# MAGIC       e.stopPropagation();
# MAGIC     }
# MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
# MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
# MAGIC       var elem = answerCodeBlocks[i];
# MAGIC       var parent = elem.parentNode;
# MAGIC       if (parent.name != "pre") {
# MAGIC         var newNode = document.createElement("pre");
# MAGIC         newNode.append(elem.cloneNode(true));
# MAGIC         elem.replaceWith(newNode);
# MAGIC         elem = newNode;
# MAGIC       }
# MAGIC       elem.ondblclick = function(e) {
# MAGIC         e.stopPropagation();
# MAGIC       };
# MAGIC 
# MAGIC       elem.style.marginTop = "1em";
# MAGIC     }
# MAGIC   };
# MAGIC </script>
# MAGIC 
# MAGIC <div>
# MAGIC   <button type="button" class="btn btn-light"
# MAGIC           style="margin-top: 1em"
# MAGIC           id="hint-button-9945">Click here for a hint</button>
# MAGIC </div>
# MAGIC <div id="answer-9945" style="padding-bottom: 20px; display: none">
# MAGIC   The answer:
# MAGIC   <div class="answer" style="margin-left: 1em">
# MAGIC When accuracy is not a significant concern - that is **Receipt Time** is close enough to **Event Time**
# MAGIC 
# MAGIC One example would be for IoT events that can be delayed by minutes but the resolution of your query is by days or months (close enough)
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowed Streaming Example</h2>
# MAGIC 
# MAGIC For this example, we will examine the files in `/mnt/training/sensor-data/accelerometer/time-series-stream.json/`.
# MAGIC 
# MAGIC Each line in the file contains a JSON record with two fields: `time` and `action`
# MAGIC 
# MAGIC New files are being written to this directory continuously (aka streaming).
# MAGIC 
# MAGIC Theoretically, there is no end to this process.
# MAGIC 
# MAGIC Let's start by looking at the head of one such file:

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/file-0.json

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Let's try to analyze these files interactively. 
# MAGIC 
# MAGIC First configure a schema.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The schema must be specified for file-based Structured Streams. 
# MAGIC Because of the simplicity of the schema, we can use the simpler, DDL-formatted, string representation of the schema.

# COMMAND ----------

inputPath = "dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/"

jsonSchema = "time timestamp, action string"

# COMMAND ----------

# MAGIC %md
# MAGIC With the schema defined, we can create the initial DataFrame `inputDf` and then `countsDF` which represents our aggregation:

# COMMAND ----------

from pyspark.sql.functions import window, col

inputDF = (spark
  .readStream                                 # Returns an instance of DataStreamReader
  .schema(jsonSchema)                         # Set the schema of the JSON data
  .option("maxFilesPerTrigger", 1)            # Treat a sequence of files as a stream, one file at a time
  .json(inputPath)                            # Specifies the format, path and returns a DataFrame
)

countsDF = (inputDF
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For the aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("count"),                       # Include count
          col("action"))                      # Include action
  .orderBy(col("start"))                      # Sort by the start time
)

# COMMAND ----------

# MAGIC %md
# MAGIC To view the results of our query, pass the DataFrame `countsDF` to the `display()` function.

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Considerations
# MAGIC 
# MAGIC If you run that query, as is, it will take a surprisingly long time to start generating data. What's the cause of the delay? 
# MAGIC 
# MAGIC If you expand the **Spark Jobs** component, you'll see something like this:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/structured-streaming-shuffle-partitions-200.png"/>
# MAGIC 
# MAGIC It's our `groupBy()`. `groupBy()` causes a _shuffle_, and, by default, Spark SQL shuffles to 200 partitions. In addition, we're doing a _stateful_ aggregation: one that requires Structured Streaming to maintain and aggregate data over time.
# MAGIC 
# MAGIC When doing a stateful aggregation, Structured Streaming must maintain an in-memory _state map_ for each window within each partition. For fault tolerance reasons, the state map has to be saved after a partition is processed, and it needs to be saved somewhere fault-tolerant. To meet those requirements, the Streaming API saves the maps to a distributed store. On some clusters, that will be HDFS. Databricks uses the DBFS.
# MAGIC 
# MAGIC That means that every time it finishes processing a window, the Streaming API writes its internal map to disk. The write has some overhead, typically between 1 and 2 seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC One way to reduce this overhead is to reduce the number of partitions Spark shuffles to.
# MAGIC 
# MAGIC In most cases, you want a 1-to-1 mapping of partitions to cores for streaming applications.

# COMMAND ----------

# MAGIC %md
# MAGIC Rerun the query below and notice the performance improvement.
# MAGIC 
# MAGIC Once the data is loaded, render a line graph with 
# MAGIC * **Keys** is set to `start`
# MAGIC * **Series groupings** is set to `action`
# MAGIC * **Values** is set to `count`

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("SS03-countsDF-p")

# COMMAND ----------

# MAGIC %md
# MAGIC When you are done, stop all the streaming jobs.

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Problem with Generating Many Windows</h2>
# MAGIC 
# MAGIC We are generating a window for every 1 hour aggregate. 
# MAGIC 
# MAGIC _Every window_ has to be separately persisted and maintained.
# MAGIC 
# MAGIC Over time, this aggregated data will build up in the driver.
# MAGIC 
# MAGIC The end result being a massive slowdown if not an OOM Error.
# MAGIC 
# MAGIC ### How do we fix that problem?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <script type="text/javascript">
# MAGIC   window.onload = function() {
# MAGIC     var allHints = document.getElementsByClassName("hint-686");
# MAGIC     var answer = document.getElementById("answer-686");
# MAGIC     var totalHints = allHints.length;
# MAGIC     var nextHint = 0;
# MAGIC     var hasAnswer = (answer != null);
# MAGIC     var items = new Array();
# MAGIC     var answerLabel = "Click here for the answer";
# MAGIC     for (var i = 0; i < totalHints; i++) {
# MAGIC       var elem = allHints[i];
# MAGIC       var label = "";
# MAGIC       if ((i + 1) == totalHints)
# MAGIC         label = answerLabel;
# MAGIC       else
# MAGIC         label = "Click here for the next hint";
# MAGIC       items.push({label: label, elem: elem});
# MAGIC     }
# MAGIC     if (hasAnswer) {
# MAGIC       items.push({label: '', elem: answer});
# MAGIC     }
# MAGIC 
# MAGIC     var button = document.getElementById("hint-button-686");
# MAGIC     if (totalHints == 0) {
# MAGIC       button.innerHTML = answerLabel;
# MAGIC     }
# MAGIC     button.onclick = function() {
# MAGIC       items[nextHint].elem.style.display = 'block';
# MAGIC       if ((nextHint + 1) >= items.length)
# MAGIC         button.style.display = 'none';
# MAGIC       else
# MAGIC         button.innerHTML = items[nextHint].label;
# MAGIC         nextHint += 1;
# MAGIC     };
# MAGIC     button.ondblclick = function(e) {
# MAGIC       e.stopPropagation();
# MAGIC     }
# MAGIC     var answerCodeBlocks = document.getElementsByTagName("code");
# MAGIC     for (var i = 0; i < answerCodeBlocks.length; i++) {
# MAGIC       var elem = answerCodeBlocks[i];
# MAGIC       var parent = elem.parentNode;
# MAGIC       if (parent.name != "pre") {
# MAGIC         var newNode = document.createElement("pre");
# MAGIC         newNode.append(elem.cloneNode(true));
# MAGIC         elem.replaceWith(newNode);
# MAGIC         elem = newNode;
# MAGIC       }
# MAGIC       elem.ondblclick = function(e) {
# MAGIC         e.stopPropagation();
# MAGIC       };
# MAGIC 
# MAGIC       elem.style.marginTop = "1em";
# MAGIC     }
# MAGIC   };
# MAGIC </script>
# MAGIC 
# MAGIC <div>
# MAGIC   <button type="button" class="btn btn-light"
# MAGIC           style="margin-top: 1em"
# MAGIC           id="hint-button-686">Click here for a hint</button>
# MAGIC </div>
# MAGIC <div id="answer-686" style="padding-bottom: 20px; display: none">
# MAGIC   The answer:
# MAGIC   <div class="answer" style="margin-left: 1em">
# MAGIC One simple solution is to increase the size of our window (say, to 2 hours).
# MAGIC 
# MAGIC That way, we&#x27;re generating fewer windows.
# MAGIC 
# MAGIC But if the job runs for a long time, we&#x27;re still building up an unbounded set of windows.
# MAGIC 
# MAGIC Eventually, we could hit resource limits.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Watermarking</h2>
# MAGIC 
# MAGIC A better solution to the problem is to define a cut-off.
# MAGIC 
# MAGIC A point after which Structured Streaming is allowed to throw saved windows away.
# MAGIC 
# MAGIC That's what _watermarking_ allows us to do.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Refining our previous example
# MAGIC 
# MAGIC Below is our previous example with watermarking. 
# MAGIC 
# MAGIC We're telling Structured Streaming to keep no more than 2 hours of aggregated data.

# COMMAND ----------

watermarkedDF = (inputDF
  .withWatermark("time", "2 hours")           # Specify a 2-hour watermark
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For each aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("count"),                       # Include count
          col("action"))                      # Include action
  .orderBy(col("start"))                      # Sort by the start time
)
display(watermarkedDF)                        # Start the stream and display it

# COMMAND ----------

# MAGIC %md
# MAGIC In the example above,   
# MAGIC * Data received 2 hour _past_ the watermark will be dropped. 
# MAGIC * Data received within 2 hours of the watermark will never be dropped.
# MAGIC 
# MAGIC More specifically, any data less than 2 hours behind the latest data processed till then is guaranteed to be aggregated.
# MAGIC 
# MAGIC However, the guarantee is strict only in one direction. 
# MAGIC 
# MAGIC Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated. 
# MAGIC 
# MAGIC The more delayed the data is, the less likely the engine is going to process it.

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("SS03-watermarkDF-p")

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all the streams

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lab, [Time Windows Lab]($./Labs/SS 03 - Time Windows Lab).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>