object Annotations {
  // Create SparkSession
  /*
  val conf = new SparkConf()
        .setAppName("Space Object Classifier")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)

      // Assuming your RDD is named 'rdd' and you have imported necessary classes

      // Define a function to map class strings to double numbers
      def mapClassToDouble(classString: String): Option[Double] = classString match {
        case "GALAXY" => Some(69.0)
        case "STAR" => Some(104.0)
        case "QSO" => Some(420.0)
        case _ => None
      }

      // Read your dataset into an RDD
      val rdd = sc.textFile("data/copia.csv")

      // Now, map each record and replace the class with double numbers
      val mappedRDD = rdd.map { line =>
        val fields = line.split(";")
        val classDoubleOption = mapClassToDouble(fields(13))
        classDoubleOption match {
          case Some(classDouble) =>
            fields.updated(13, classDouble.toString).mkString(";")
          case None =>
            // Handle unknown class values here
            // You might want to log these or handle them differently based on your use case
            line
        }
      }

      // Show the result
      mappedRDD.take(10).foreach(println)

      // Stop the SparkSession
      sc.stop()
  */

  /*
    // Assuming 'data' is your RDD containing the dataset
    val header = dataWithHeader.first() // Get the first row (header row)
    val columnNames = header.split(";") // Assuming the columns are comma-separated
    columnNames.foreach(println) // Print the column names

    // Filter out the header row
    val data = dataWithHeader.filter(row => row != header)
    data.take(5).foreach(println)
   */

  /*
// Parse and preprocess the data
val parsedData = data.map { line =>
  val parts = line.split(",")
  try {
    val features = parts.slice(1, 17).map { value =>
      // Handle missing values by replacing them with a default value
      if (value.isEmpty) {
        // Replace missing values with a default value (e.g., 0.0)
        0.0
      } else {
        // Convert non-missing values to Double
        value.toDouble
      }
    }
    val label = parts(17) match {
      case "galaxy" => 0.0
      case "star" => 1.0
      case "quasar" => 2.0
    }
    Some(LabeledPoint(label, Vectors.dense(features)))
  } catch {
    case _: Throwable =>
      println(s"Error parsing line: $line")
      None
  }
}.filter(_.isDefined).map(_.get)
*/

  /*
  object Main {
    def main(args: Array[String]): Unit = {
      // Initialize SparkContext
      val conf = new SparkConf()
        .setAppName("Space Object Classifier")
        .setMaster("local[*]")

      val sc = new SparkContext(conf)

      // Define a function to map class strings to double numbers
      def mapClassToDouble(classString: String): Option[Double] = classString match {
        case "GALAXY" => Some(69.0)
        case "STAR" => Some(104.0)
        case "QSO" => Some(420.0)
        case _ => None
      }

      // Read the CSV file into an RDD of strings, where each string represents a row in the CSV file
      val rdd = sc.textFile("data/copia.csv")
      //.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } // Skip header
      // Print the first few lines of the RDD
      rdd.take(5).foreach(println)

      // Now, map each record and replace the class with double numbers
      val mappedRDD = rdd.map { line =>
        val fields = line.split(";")

        println(fields(13).getClass.getSimpleName)

        val classDoubleOption = mapClassToDouble(fields(13))
        classDoubleOption match {
          case Some(classDouble) =>
            fields.updated(13, classDouble.toString).mkString(";")
          case None =>
            // Handle unknown class values here
            // You might want to log these or handle them differently based on your use case
            line
        }
      }

      // Show the result
      mappedRDD.take(10).foreach(println)

      val header = mappedRDD.first() // Get the first row (header row)
      val columnNames = header.split(";") // Assuming the columns are comma-separated
      columnNames.foreach(println) // Print the column names

      // Filter out the header row
      val data = mappedRDD.filter(row => row != header)
      data.take(5).foreach(println)


      val parsedData = data.map { line =>
        val parts = line.split(";") // Split using semicolons as the delimiter
        if (parts.length == 18) {
          try {
            println(parts(13).getClass.getSimpleName)

            val features = parts.slice(1, 18).map { value =>
              // Handle missing values and scientific notation
              val numericValue = if (value.isEmpty) 0.0 else value.toDouble
              numericValue
            }

            // val label = mapClassToDouble(parts(13)) match {
            val label1 = mapClassToDouble(parts(13))
            val label = label1 match {
              case Some(classDouble) =>
                classDouble
              case None =>
                // Handle unknown class values here
                // You might want to log these or handle them differently based on your use case
                println(s"Unknown class: ${parts(13)}")
                0.0 // Default value or handle it according to your needs
            }
            Some(LabeledPoint(label, Vectors.dense(features)))
          }
          catch {
            case _: Throwable =>
              println(s"Error parsing line: $line")
              None
          }
        }
        else {
          println(s"Invalid line format: $line")
          None
        }
      }.filter(_.isDefined).map(_.get)

      parsedData.take(1).foreach(println)

      // Split data into training and testing sets
      val Array(trainData, testData) = parsedData.randomSplit(Array(0.8, 0.2), seed = 42)

      // Train the model
      val numClasses = 3
      val categoricalFeaturesInfo = Map[Int, Int]() // No categorical features
      val numTrees = 10
      val featureSubsetStrategy = "auto" // Let the algorithm choose
      val impurity = "gini"
      val maxDepth = 4
      val maxBins = 32

      val model = RandomForest.trainClassifier(
        trainData,
        numClasses,
        categoricalFeaturesInfo,
        numTrees,
        featureSubsetStrategy,
        impurity,
        maxDepth,
        maxBins
      )

      // Stop SparkSession
      sc.stop()
    }
  }
  */

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

  /*
object SequentialDecisionTree {
def main(args: Array[String]): Unit = {
  val data = loadData("data/star_classification.csv")

  val (trainData, testData) = trainTestSplit(data, 0.8)

  // Measure the time taken to train the Decision Tree
  val (tree, trainingTime) = measureTime {
    DecisionTree.train(trainData, maxDepth = 10, minSize = 5)
  }

  val predictions = testData.map(dp => (dp.label, tree.predict(dp.features)))
  val accuracy = evaluate(predictions)
}

private def loadData(filePath: String): Array[DataPoint] = {
  Using(Source.fromFile(filePath)) { source =>
    source.getLines().drop(1).map { line =>
      val values = line.split(",").map(_.toDouble)
      DataPoint(values.init, values.last)
    }.toArray
  }.getOrElse(Array.empty[DataPoint])
}

private def trainTestSplit(data: Array[DataPoint], trainRatio: Double): (Array[DataPoint], Array[DataPoint]) = {
  val shuffled = scala.util.Random.shuffle(data.toList)
  val splitIndex = (data.length * trainRatio).toInt
  (shuffled.take(splitIndex).toArray, shuffled.drop(splitIndex).toArray)
}
*/

}
