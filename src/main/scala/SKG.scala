import java.lang.Double.doubleToLongBits
import java.lang.Long.bitCount
import scala.annotation.tailrec
import scala.util.Random

class SKG(scale: Int, ratio: Int, a: Double, b: Double, c: Double, d: Double) extends Serializable {
  @inline final val random = new Random()

  @inline final val numVertices = Math.pow(2, scale).toLong
  @inline final val numEdges = ratio * numVertices

  @inline final val abcd = Array.tabulate(scale + 1)(x => math.pow(a + b, scale - x) * math.pow(c + d, x))
  @inline final val aab = Array.tabulate(scale + 1)(x => math.pow(a / (a + b), x))
  @inline final val ccd = Array.tabulate(scale + 1)(x => math.pow(c / (c + d), x))

  @inline final def bitSum(s: Long): Int = bitCount(s)

  def getPout(vertexId: Long): Double = abcd(bitSum(vertexId))

  def getExpectedDegree(vid: Long): Double = numEdges * getPout(vid)

  def getDegree(vertexId: Long, r: Random = random): Long = {
    val s = getExpectedDegree(vertexId: Long)
    math.round(s + math.sqrt(s * (1 - getPout(vertexId))) * r.nextGaussian)
  }

  def getCDF(vertexId: Long, logTo: Int): Double = {
    val bs = bitSum(vertexId >>> logTo)
    val aab = this.aab(scale - logTo - bs)
    val ccd = this.ccd(bs)
    getPout(vertexId) * aab * ccd
  }

  def getRecVec(vid: Long): Array[Double] = {
    val array = new Array[Double](scale + 1)
    var i = 0
    while (i <= scale) {
      array(i) = getCDF(vid, i)
      i += 1
    }
    array
  }

  @inline final def getSigmas(recVec: Array[Double]): Array[Double] = {
    val array = new Array[Double](scale)
    var i = 0
    while (i < scale) {
      array(i) = recVec(i) / (recVec(i + 1) - recVec(i))
      i += 1
    }
    array
  }

  @inline
  @tailrec final def determineEdge0BinarySearch(gp: Double, recVec: Array[Double], sigmas: Array[Double], prev: Int, acc: Long = 0): Long = {
    val k = binarySearch(recVec, prev, gp)
    if (0 > k || k >= prev)
      acc
    else
      determineEdge0BinarySearch(sigmas(k) * (gp - recVec(k)), recVec, sigmas, k + 1, (1L << k) + acc)
  }

  def determineEdge(recVec: Array[Double], sigmas: Array[Double], r: Random = random): Long = {
    determineEdge0BinarySearch(r.nextDouble * recVec.last, recVec, sigmas, scale)
  }

  @inline final def binarySearch(array: Array[Double], toIndex: Int, key: Double): Int = {
    var low = 0
    var high = toIndex - 1
    while (low <= high) {
      val mid = (low + high) >>> 1
      val midVal = array(mid)

      if (midVal < key)
        low = mid + 1 // Neither val is NaN, thisVal is smaller
      else if (midVal > key)
        high = mid - 1 // Neither val is NaN, thisVal is larger
      else {
        val midBits = doubleToLongBits(midVal)
        val keyBits = doubleToLongBits(key)
        if (midBits == keyBits) { // Values are equal
          mid // Key found
        } else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
          low = mid + 1
        else // (0.0, -0.0) or (NaN, !NaN)
          high = mid - 1
      }
    }
    low - 1
  }
}