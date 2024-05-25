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

}
