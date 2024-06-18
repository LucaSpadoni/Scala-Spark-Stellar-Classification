
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

    val splits = for {
      featureIndex <- 0 until numFeatures
      threshold <- data.map(_.features(featureIndex)).distinct.sorted.init
    } yield {
      val (left, right) = data.partition(_.features(featureIndex) <= threshold)
      val gini = calculateGini(left) * left.length / data.length + calculateGini(right) * right.length / data.length
      (featureIndex, threshold, gini)
    }

    if (splits.nonEmpty) {
      val (bestFeature, bestThreshold, _) = splits.minBy(_._3)
      (bestFeature, bestThreshold)
    } else {
      (-1, Double.NaN)
    }
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