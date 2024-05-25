import scala.io.Source
import scala.util.{Using, Try}
import Utils.measureTime

class DecisionTree(val featureIndex: Int = -1, val threshold: Double = Double.NaN,
                   val left: DecisionTree = null, val right: DecisionTree = null,
                   val prediction: Double = Double.NaN) {

  private def isLeaf: Boolean = left == null && right == null

  def predict(features: Array[Double]): Double = {
    if (isLeaf)
      prediction
    else {
      if (features(featureIndex) <= threshold)
        left.predict(features)
      else
        right.predict(features)
    }
  }
}

object DecisionTree {
  def train(data: Array[DataPoint], maxDepth: Int, minSize: Int): DecisionTree = {
    if (data.isEmpty || maxDepth == 0 || data.length <= minSize) {
      createLeaf(data)
    }
    else {
      val (featureIndex, threshold) = findBestSplit(data)

      if (featureIndex == -1) {
        createLeaf(data)
      }
      else {
        val (leftData, rightData) = data.partition(_.features(featureIndex) <= threshold)

        new DecisionTree(
          featureIndex,
          threshold,
          train(leftData, maxDepth - 1, minSize),
          train(rightData, maxDepth - 1, minSize)
        )
      }
    }
  }

  // Calculate the majority label and create a new DecisionTree instance with a prediction
  private def createLeaf(data: Array[DataPoint]): DecisionTree = {
    val majorityLabel = data.groupBy(_.label).maxBy(_._2.length)._1
    new DecisionTree(prediction = majorityLabel)
  }

  private def findBestSplit(data: Array[DataPoint]): (Int, Double) = {
    val numFeatures = data.head.features.length

    val (bestFeature, bestThreshold, bestGini) = (0 until numFeatures).flatMap { featureIndex =>
      val thresholds = data.map(_.features(featureIndex)).distinct.sorted
      thresholds.init.map { threshold =>
        val (left, right) = data.partition(_.features(featureIndex) <= threshold)
        val gini = calculateGini(left) * left.length / data.length + calculateGini(right) * right.length / data.length
        (featureIndex, threshold, gini)
      }
    }.minByOption(_._3).getOrElse((-1, Double.NaN, Double.MaxValue))
    (bestFeature, bestThreshold)
  }

  private def calculateGini(data: Array[DataPoint]): Double = {
    val total = data.length.toDouble
    data.groupBy(_.label).map { case (_, group) =>
      val proportion = group.length / total
      proportion * (1 - proportion)
    }.sum
  }

  def evaluate(predictions: Array[(Double, Double)]): Double = {
    val correct = predictions.count { case (label, prediction) => label == prediction }
    correct.toDouble / predictions.length
  }
}

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
