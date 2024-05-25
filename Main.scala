import org.apache.spark.input.PortableDataStream
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import Utils.measureTime

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Space Object Classifier")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schema = defineSchema()

    // Read the dataset and pre-process it
    val df = readData(spark, schema)
    val df2 = transformClassColumn(df)
    val assembledDf = assembleFeatures(df2)

    // Randomly split the provided features (the dataset minus the class column) for training and testing
    val Array(trainData, testData) = assembledDf.randomSplit(Array(0.8, 0.2))
    println(s"\n Training set count: ${trainData.count()}, Test set count: ${testData.count()}")

    // Train and evaluate the Spark parallel Random Forest model
    val (rfModel, rfTime) = trainRandomForest(trainData)
    println("\n Random Forest model training successfully terminated.")
    println("\n Making predictions on test data...")
    val predictions = rfModel.transform(testData)
    predictions.show(5)
    val sparkAccuracy = evaluateModel(predictions)
    println(s"\n Parallel Random Forest Accuracy = $sparkAccuracy")
    println(s" Parallel Random Forest training time: $rfTime seconds")

    // Train and evaluate the sequential Decision Tree model
    val (sequentialTrainData, sequentialTestData) = prepareSequentialData(trainData, testData)
    println("\n Training the sequential Decision Tree model...")
    val (sequentialTree, dtTime) = measureTime {
      DecisionTree.train(sequentialTrainData, maxDepth = 10, minSize = 5)
    }
    println("\n Decision Tree model training successfully terminated.")
    println("\n Making predictions on test data...")
    val sequentialPredictions = sequentialTestData.map(dp => (dp.label, sequentialTree.predict(dp.features)))
    val sequentialAccuracy = DecisionTree.evaluate(sequentialPredictions)
    println(s"\n Sequential Decision Tree Accuracy = $sequentialAccuracy")
    println(s" Sequential Decision training time: $dtTime seconds")

    // Example custom input, the prediction should result in a QUASAR (3.0)
    val customInput = Array(12345678, 1.494388639357, 3.29174632998873, 20.38562, 20.40514, 20.29996, 20.05918, 19.89044, 7712, 301, 5, 339, 9.84382410307275E18, 2.031528, 8743, 57663, 295)
    println("\n Making a prediction on custom input...")
    predictCustomInput(spark, rfModel, customInput, df2.columns.filter(_ != "class"))

    println("\n Stopping the Spark session.")
    spark.stop()
  }

  private def defineSchema(): StructType = {
    StructType(
      Seq(
        StructField("obj_ID", DoubleType, nullable = true),
        StructField("alpha", DoubleType, nullable = true),
        StructField("delta", DoubleType, nullable = true),
        StructField("u", DoubleType, nullable = true),
        StructField("g", DoubleType, nullable = true),
        StructField("r", DoubleType, nullable = true),
        StructField("i", DoubleType, nullable = true),
        StructField("z", DoubleType, nullable = true),
        StructField("run_ID", IntegerType, nullable = true),
        StructField("rerun_ID", IntegerType, nullable = true),
        StructField("cam_col", IntegerType, nullable = true),
        StructField("field_ID", IntegerType, nullable = true),
        StructField("spec_obj_ID", DoubleType, nullable = true),
        StructField("class", StringType, nullable = true),
        StructField("redshift", DoubleType, nullable = true),
        StructField("plate", IntegerType, nullable = true),
        StructField("MJD", IntegerType, nullable = true),
        StructField("fiber_ID", IntegerType, nullable = true)
      )
    )
  }

  private def readData(spark: SparkSession, schema: StructType): DataFrame = {
    println("\n Reading the dataset...")

    spark.read
      .option("header", true)
      .option("delimiter", ",")
      .schema(schema)
      .csv("data/star_classification.csv")
      .limit(100000) // Only read the first 10000 rows for testing
      .na.drop() // Drop rows with null values
  }

  private def transformClassColumn(df: DataFrame): DataFrame = {
    println("\n Transforming the class column...")

    val new_col = when(col("class") === "GALAXY", 1.0)
      .otherwise(when(col("class") === "STAR", 2.0)
        .otherwise(3.0))
    df.withColumn("class", new_col)
  }

  private def assembleFeatures(df: DataFrame): DataFrame = {
    val featureCols = df.columns.filter(_ != "class")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    assembler.transform(df)
  }

  private def trainRandomForest(trainData: DataFrame): (RandomForestClassificationModel, Double) = {
    println("\n Training the parallel Random Forest model...")

    val (model, elapsedTime) = measureTime {
      val rf = new RandomForestClassifier()
        .setLabelCol("class")
        .setFeaturesCol("features")
        .setNumTrees(10)
      rf.fit(trainData)
    }

    (model, elapsedTime)
  }

  private def evaluateModel(predictions: DataFrame): Double = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("class")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    evaluator.evaluate(predictions)
  }

  // Convert DataFrame rows to DataPoint instances using the rowToDataPoint helper method
  private def prepareSequentialData(trainData: DataFrame, testData: DataFrame): (Array[DataPoint], Array[DataPoint]) = {
    val trainDataPoints = trainData.collect().map(rowToDataPoint)
    val testDataPoints = testData.collect().map(rowToDataPoint)
    (trainDataPoints, testDataPoints)
  }

  private def rowToDataPoint(row: Row): DataPoint = {
    DataPoint(
      row.getAs[org.apache.spark.ml.linalg.Vector]("features").toArray,
      row.getDouble(row.fieldIndex("class"))
    )
  }

  private def predictCustomInput(spark: SparkSession, model: RandomForestClassificationModel, customInput: Array[Double], featureCols: Array[String]): Unit = {
    // Ensure customInput has the correct number of features
    assert(customInput.length == featureCols.length, s"Expected ${featureCols.length} features, but got ${customInput.length}")

    // Convert the input array to an RDD
    val customRow = Row.fromSeq(customInput)
    val customRDD = spark.sparkContext.parallelize(Seq(customRow))

    // Create DataFrame from RDD
    val customSchema = StructType(featureCols.map(StructField(_, DoubleType, nullable = false)))
    val customDF = spark.createDataFrame(customRDD, customSchema)

    // Assemble features
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val customAssembled = assembler.transform(customDF)

    // Make prediction
    val customPrediction = model.transform(customAssembled)

    // Show the prediction result
    customPrediction.select("prediction").show()
  }
}



/*
        val spark = SparkSession.builder()
          .appName("Space Object Classifier")
          .master("local[*]")
          .getOrCreate()

        import spark.implicits._

        // Define the schema for your data
        val schema = StructType(
          Seq(
            StructField("obj_ID", DoubleType, nullable = true),
            StructField("alpha", DoubleType, nullable = true),
            StructField("delta", DoubleType, nullable = true),
            StructField("u", DoubleType, nullable = true),
            StructField("g", DoubleType, nullable = true),
            StructField("r", DoubleType, nullable = true),
            StructField("i", DoubleType, nullable = true),
            StructField("z", DoubleType, nullable = true),
            StructField("run_ID", IntegerType, nullable = true),
            StructField("rerun_ID", IntegerType, nullable = true),
            StructField("cam_col", IntegerType, nullable = true),
            StructField("field_ID", IntegerType, nullable = true),
            StructField("spec_obj_ID", DoubleType, nullable = true),
            StructField("class", StringType, nullable = true),
            StructField("redshift", DoubleType, nullable = true),
            StructField("plate", IntegerType, nullable = true),
            StructField("MJD", IntegerType, nullable = true),
            StructField("fiber_ID", IntegerType, nullable = true)
          )
        )

        // Read CSV into DataFrame with defined schema
        val df = spark.read
          .option("header", true)
          .option("delimiter", ",")
          .schema(schema)
          .csv("data/star_classification.csv")
          .na.drop() // Drop rows with null values

        df.show()

        // Change the values of class column into double
        val new_col = when(col("class") === "GALAXY", 1.0)
          .otherwise(when(col("class") === "STAR", 2.0)
          .otherwise(3.0))

        val df2 = df.withColumn("class", new_col)

        df2.show()

        // Assemble features into a single column
        val featureCols = df2.columns.filter(_ != "class")
        val assembler = new VectorAssembler()
          .setInputCols(featureCols)
          .setOutputCol("features")

        val assembledDf = assembler.transform(df2)

        // Split data into training and testing sets
        val Array(trainData, testData) = assembledDf.randomSplit(Array(0.8, 0.2))

        // Define and train Random Forest classifier
        val rf = new RandomForestClassifier()
          .setLabelCol("class")
          .setFeaturesCol("features")
          .setNumTrees(10) // Number of trees in the forest

        val model = rf.fit(trainData)

        // Make predictions on test data
        val predictions = model.transform(testData)

        // Select example rows to display
        predictions.select("class", "features").show(5)

        // Evaluate model
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("class")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(predictions)
        println(s"Accuracy = ${accuracy}")
        println(s"Test Error = ${(1.0 - accuracy)}")

        ////////////////////////////////////////////////////////////////////////////////////////////////
        // Class-specific Metrics:
        // Convert predictions DataFrame to RDD of (prediction, label) pairs
        val predictionAndLabels = predictions.select("prediction", "class").rdd.map {
          row => (row.getDouble(0), row.getDouble(1))
        }

        // Instantiate MulticlassMetrics
        val metrics = new MulticlassMetrics(predictionAndLabels)

        // Compute metrics for each class
        val labels = metrics.labels
        println("\n \nClass-specific Metrics:")
        labels.foreach { label =>
          println(s"Class $label precision: ${metrics.precision(label)}")
          println(s"Class $label recall: ${metrics.recall(label)}")
          println(s"Class $label F1-score: ${metrics.fMeasure(label)}")
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////
        // Feature Importance (for Random Forest):
        // This score indicates how much the feature contributes to the model's predictions. A higher importance score suggests that the
        // feature has a greater impact on the model's decisions.
        val featureImportance = model.featureImportances.toArray.zipWithIndex
          .sortBy(-_._1) // Sort feature importance in descending order

        // Print feature importance (score).
        println("\n \nFeature Importance:")
        featureImportance.foreach { case (importance, index) =>
          println(s"Feature $index: $importance")
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////
        // Function to make predictions on custom input
        def predictCustomInput(customInput: Array[Double]): Unit = {
          // Ensure customInput has the correct number of features
          assert(customInput.length == featureCols.length, s"Expected ${featureCols.length} features, but got ${customInput.length}")

          // Convert the input array to an RDD
          val customRow = Row.fromSeq(customInput)
          val customRDD: RDD[Row] = spark.sparkContext.parallelize(Seq(customRow))

          // Create DataFrame from RDD
          val customDF = spark.createDataFrame(customRDD, StructType(featureCols.map(StructField(_, DoubleType, nullable = false))))

          // Assemble features
          val customAssembled = assembler.transform(customDF)

          // Make prediction
          val customPrediction = model.transform(customAssembled)

          // Show the prediction result
          customPrediction.select("prediction").show()
        }

        // Example custom input, the prediction should result in a QUASAR (3.0)
        val customInput = Array(12345678, 1.494388639357, 3.29174632998873, 20.38562, 20.40514, 20.29996, 20.05918, 19.89044, 7712, 301, 5, 339, 9.84382410307275E18, 2.031528, 8743, 57663, 295)
        predictCustomInput(customInput)

        // Stop SparkSession
        spark.stop()
    */