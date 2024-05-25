object Utils {
  def measureTime[T](block: => T): (T, Double) = {
    val startTime = System.currentTimeMillis()
    val result = block
    val endTime = System.currentTimeMillis()
    val elapsedTime = (endTime - startTime) / 1000.0 // Time in seconds
    (result, elapsedTime)
  }
}