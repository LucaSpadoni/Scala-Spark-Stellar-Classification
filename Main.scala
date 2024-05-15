import org.apache.spark.input.PortableDataStream
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
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

    // Stop SparkSession
    spark.stop()
  }
}
