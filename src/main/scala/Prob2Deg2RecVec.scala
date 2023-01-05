import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.hadoop.io.LongWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileWriter}
import scala.util.Random

object Prob2Deg2RecVec {
  val t: Double = 2.01
  val c: Double = (2 * (t - 1)) / (1 + t)
  val undirected = false
  var numEdges: Int = 0

  def main(args: Array[String]): Unit = {
    val scale = if (args.length > 0) args(0).toInt else 20
    val ratio = if (args.length > 1) args(1).toInt else 16
    val path = if (args.length > 2) args(2) else System.currentTimeMillis().toString
    val machines = if (args.length > 3) args(3).toInt else 16

    val (a, b, c, d) = (0.57d, 0.19d, 0.19d, 0.05d)

    val numVertices = math.pow(2, scale).toInt
    numEdges = ratio * numVertices

    val rng: Long = System.currentTimeMillis

    println(s"Probabilities=($a, $b, $c, $d), |V|=$numVertices (2 ^ $scale), |E|=$numEdges ($ratio * $numVertices)")
    println(s"PATH=$path, Machine=$machines")
    println(s"RandomSeed=$rng")

    val conf = new SparkConf().setAppName("Prob2Deg2RecVec")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()

    val ds = sc.broadcast(new SKG(scale, ratio, a, b, c, d))
    val degreeRDD = sc.parallelize(generateArrayPowerLaw(numVertices).zipWithIndex)
    val edges = degreeRDD.doRecVecGen(ds, rng)
    edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], classOf[TSVOutputFormat])

    val timeSpent = System.currentTimeMillis() - startTime

    val log =
      s"""Prob2Deg2RecVec
         |Scale $scale
         |Generation completed in ${timeSpent / 1000d} seconds""".stripMargin

    println(log)
    val fileWriter = new FileWriter(new File(path, s"scale$scale-${timeSpent / 1000d}s.txt"))
    fileWriter.write(log)
    fileWriter.close()

    sc.stop
  }

  implicit class RecVecGenClass(self: RDD[(Int, Int)]) extends Serializable {
    def doRecVecGen(ds: Broadcast[_ <: SKG], rng: Long): RDD[(Long, LongOpenHashBigSet)] = {
      self.mapPartitions { partitions =>
        val skg = ds.value
        partitions.flatMap { case (degree, u) =>
          val random = new Random(rng + u)
          if (degree < 1)
            Iterator.empty
          else {
            val recVec = skg.getRecVec(u)
            val sigmas = skg.getSigmas(recVec)
            val adjacency = new LongOpenHashBigSet(degree)
            var i = 0
            while (i < degree) {
              adjacency.add(skg.determineEdge(recVec, sigmas, random))
              i += 1
            }
            Iterator((u, adjacency))
          }
        }
      }
    }
  }

  implicit class RangePartitionFromDegreeRDD(self: RDD[(Int, Int)]) extends Serializable {
    def rangePartition(numMachines: Int, numVertices: Int, numEdges: Int): RDD[Int] = {
      val sc = self.sparkContext
      val accumulator = new SetAccumulatorV2()
      sc.register(accumulator, "SetAccumulatorV2")
      val lastGlobal = self.fold((0, 0)) { (left, right) =>
        val first = if (left._1 > right._1) right else left
        val second = if (left._1 > right._1) left else right
        if (first._2 > (numEdges / numMachines / 100)) {
          accumulator.add(first)
          second
        } else (second._1, first._2 + second._2)
      }
      accumulator.add(lastGlobal)
      val sorted = accumulator.value.toSeq.sortBy { case (vid, _) => vid }
      val range = for (i <- 0 until sorted.length - 1)
        yield if (sorted(i)._1 <= sorted(i + 1)._1 - 1)
          (sorted(i)._1, sorted(i + 1)._1 - (if (i == sorted.length - 2) 0 else 1))
        else (-1, -1)
      val range2: IndexedSeq[((Int, Int), Int)] = range.filter(p => p._1 >= 0 && p._2 >= 0).zipWithIndex
      val range2finalize = range2.map { case ((f, s), i) => if (i == range2.length - 1) ((f, numVertices - 1), i) else ((f, s), i) }
      val range3 = range2finalize.map { case ((st, ed), _) => (st, ed) }
      val rangeRDD = sc.parallelize(range3, numMachines)
      val threshold = (Int.MaxValue / 4).toInt
      val rangeRDD2 = rangeRDD.flatMap {
        case (f, s) =>
          val end = math.ceil((s - f + 1).toDouble / threshold.toDouble).toInt
          if (end == 1) Iterable((f, s))
          else {
            val array = new Array[(Int, Int)](end)
            var i = 0
            while (i + 1 < end) {
              array(i) = (f + threshold * i, f + threshold * (i + 1) - 1)
              i += 1
            }
            array(end - 1) = (f + threshold * (end - 1), s)
            array
          }
      }.flatMap(x => x._1 to x._2)
      rangeRDD2
    }
  }

  class SetAccumulatorV2(initialValue: Set[(Int, Int)] = Set((0, 0))) extends AccumulatorV2[(Int, Int), Set[(Int, Int)]] {
    private var set: Set[(Int, Int)] = initialValue

    override def isZero: Boolean = set.size == 1

    override def copy(): AccumulatorV2[(Int, Int), Set[(Int, Int)]] = new SetAccumulatorV2(set)

    override def reset(): Unit = { set = Set((0, 0)) }

    override def add(v: (Int, Int)): Unit = set += v

    override def merge(other: AccumulatorV2[(Int, Int), Set[(Int, Int)]]): Unit = set ++ other.value

    override def value: Set[(Int, Int)] = set
  }

  def generateProbabilities(n: Int, limit: Int): Array[Double] = {
    var probabilitiesSum: Double = 0
    val probabilities = new Array[Double](limit)
    probabilities(0) = 0 // leave it just to make it explicit

    for (i <- 1 until n) {
      val p: Double = c * math.pow(i, -t)
      if (i < limit) probabilities(i) = p
      probabilitiesSum += p
    }

    // normalize
    var cumulativeProb: Double = 0
    for (i <- 1 until limit) {
      cumulativeProb += probabilities(i) / probabilitiesSum
      probabilities(i) = cumulativeProb
    }
    probabilities
  }

  def generateDegrees(n: Int, limit: Int, cumProbArr: Array[Double]): Array[Int] = {
    val frequencies: Array[Int] = new Array[Int](limit)
    val x0 = 0
    val x1 = cumProbArr(limit - 1)
    val random = new Random()
    var r: Double = 0.0
    var i, j: Int = 0
    var edgesT = 0

    //    for (k <- 0 until limit)  // Array are already initialized with 0's in Scala
    //      frec(k) = 0                  // so setting them again is not necessary

    while (i < n) {
      r = math.pow((math.pow(x1, t + 1) - math.pow(x0, t + 1)) * random.nextDouble() + math.pow(x0, t + 1), 1 / (t + 1))
      j = 1
      while (j < limit) {
        if (r > cumProbArr(j - 1) && r < cumProbArr(j)) {
          frequencies(j) += 1
          edgesT += j
          j = limit
        }
        j += 1
      }
      if (undirected) { // undirected graph
        if (i == n && edgesT < numUndirectedEdges)
          i -= 1
      } else { // directed graph
        if (edgesT >= numEdges)
          i = n + 1
      }
      i += 1
    }
    frequencies
  }

  def generateArrayPowerLaw(n: Int): Array[Int] = {
    val degrees: Array[Int] = new Array[Int](n)
    val limit: Int = maxDegree(n, t)
    val probabilities = generateProbabilities(n, limit + 1)
    val frequencies = generateDegrees(n, limit + 1, probabilities)
    var h: Int = 0
    var k = limit
    while (k >= 1) {
      if (frequencies(k) > 0) {
        var l = 0
        while (l < frequencies(k)) {
          degrees(h) = k
          h += 1
          if (h >= n)
            l = frequencies(k)
          l += 1
        }
      }
      if (h >= n)
        k = 0
      k -= 1
    }
    degrees
  }

  def maxDegree(n: Int, t: Double): Int = (constant(t) * math.pow(n, exponent(t))).toInt

  def constant(t: Double): Double = 5.5 * math.pow(math.E, -math.pow(math.log(t) - 0.73, 2) / 0.04374) + 3.0321

  def exponent(t: Double): Double = math.pow(math.E, (-t + 1.54) * 5.05) + 0.457

  def numEdges(n: Int, t: Double): Int = ((n * (t - 1) * (2 * math.pow(n, 2 - t) - t)) / ((1 + t) * (2 - t))).toInt

  def numUndirectedEdges: Int = 2 * numEdges
}