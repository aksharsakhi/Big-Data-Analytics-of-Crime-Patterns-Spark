package bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * 23CSE352: Big Data Analytics - Project Review 3
 * Apache Spark & Scala RDD Implementation
 * Real-World City of Chicago Crime Pattern Analytics
 *
 * Demonstrates:
 * 1. RDD Creation from dataset (textFile)
 * 2. Transformations: filter(), map(), flatMap(), reduceByKey(), sortBy()
 * 3. Actions: count(), take(), collect(), reduce()
 * 4. Key-Value RDD operations: reduceByKey(), mapValues()
 * 5. Spark Partitions inspection & repartitioning
 * 6. Spark Lineage & DAG inspection (toDebugString)
 * 7. In-memory Persistence & Caching benchmark (cache vs persist)
 */
object CrimeAnalyticsRDD {

  case class CrimeRecord(
    id: String,
    caseNumber: String,
    date: String,
    block: String,
    iucr: String,
    primaryType: String,
    description: String,
    locationDescription: String,
    arrest: Boolean,
    domestic: Boolean,
    beat: String,
    district: String,
    ward: String,
    communityArea: String,
    latitude: String,
    longitude: String
  )

  def parseCsvLine(line: String): Array[String] = {
    val list = new scala.collection.mutable.ArrayBuffer[String]()
    val sb = new StringBuilder()
    var inQuotes = false
    var i = 0
    while (i < line.length) {
      val c = line.charAt(i)
      if (c == '\"') {
        inQuotes = !inQuotes
      } else if (c == ',' && !inQuotes) {
        list += sb.toString()
        sb.clear()
      } else {
        sb.append(c)
      }
      i += 1
    }
    list += sb.toString()
    list.toArray
  }

  def main(args: Array[String]): Unit = {
    val datasetPath = if (args.length > 0) args(0) else "dataset/chicago_crimes_clean.csv"

    println("================================================================================")
    println(" 23CSE352: Big Data Analytics - Project Review 3")
    println(" Apache Spark & Scala RDD Processing Pipeline")
    println(" Dataset: " + datasetPath)
    println("================================================================================\n")

    val conf = new SparkConf()
      .setAppName("ChicagoCrimeAnalyticsRDD")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      // --------------------------------------------------------------------------
      // STEP 1: RDD Creation & Partitions Inspection
      // --------------------------------------------------------------------------
      println("[STEP 1] Creating RDD from Real-World Chicago Crimes Dataset")
      val rawRdd = sc.textFile(datasetPath)
      val initialPartitions = rawRdd.getNumPartitions
      println(s"         -> Initial raw RDD partitions: $initialPartitions")

      // Extract header
      val header = rawRdd.first()
      println(s"         -> CSV Header: $header")

      // --------------------------------------------------------------------------
      // STEP 2: Transformations
      // --------------------------------------------------------------------------
      println("\n[STEP 2] Applying Transformations")

      // Transformation 1: filter() - Remove header and empty lines
      val dataLinesRdd = rawRdd.filter(line => line.nonEmpty && line != header)
      println(s"         -> [Transformation 1: filter()] Removed header line.")

      // Transformation 2: map() - Parse CSV into structured CrimeRecord instances
      val crimesRdd = dataLinesRdd.map(line => {
        val tokens = parseCsvLine(line)
        CrimeRecord(
          id = if (tokens.length > 0) tokens(0).trim else "",
          caseNumber = if (tokens.length > 1) tokens(1).trim else "",
          date = if (tokens.length > 2) tokens(2).trim else "",
          block = if (tokens.length > 3) tokens(3).trim else "",
          iucr = if (tokens.length > 4) tokens(4).trim else "",
          primaryType = if (tokens.length > 5) tokens(5).trim.toUpperCase else "UNKNOWN",
          description = if (tokens.length > 6) tokens(6).trim else "",
          locationDescription = if (tokens.length > 7) tokens(7).trim.toUpperCase else "OTHER",
          arrest = if (tokens.length > 8) tokens(8).trim.equalsIgnoreCase("true") else false,
          domestic = if (tokens.length > 9) tokens(9).trim.equalsIgnoreCase("true") else false,
          beat = if (tokens.length > 10) tokens(10).trim else "",
          district = if (tokens.length > 11) {
            val d = tokens(11).trim
            if (d.length == 1) "00" + d else if (d.length == 2) "0" + d else d
          } else "000",
          ward = if (tokens.length > 12) tokens(12).trim else "",
          communityArea = if (tokens.length > 13) tokens(13).trim else "",
          latitude = if (tokens.length > 19) tokens(19).trim else "",
          longitude = if (tokens.length > 20) tokens(20).trim else ""
        )
      })
      println(s"         -> [Transformation 2: map()] Parsed lines into strongly-typed CrimeRecord objects.")

      // --------------------------------------------------------------------------
      // STEP 3: Caching & Persistence Benchmark (cache vs persist)
      // --------------------------------------------------------------------------
      println("\n[STEP 3] Demonstrating Spark Persistence & Caching Benchmark")
      
      // Uncached Action Timing
      val t0Uncached = System.currentTimeMillis()
      val uncachedCount = crimesRdd.count()
      val t1Uncached = System.currentTimeMillis()
      val uncachedTime = t1Uncached - t0Uncached
      println(s"         -> Action 1 [count()] Without Cache: $uncachedCount records in ${uncachedTime} ms")

      // Apply cache() (Memory only) and persist(MEMORY_AND_DISK)
      crimesRdd.persist(StorageLevel.MEMORY_AND_DISK)
      println(s"         -> Persisted crimesRdd in MEMORY_AND_DISK.")

      // Materialize the cache with an action
      crimesRdd.count()

      // Cached Action Timing
      val t0Cached = System.currentTimeMillis()
      val cachedCount = crimesRdd.count()
      val t1Cached = System.currentTimeMillis()
      val cachedTime = t1Cached - t0Cached
      println(s"         -> Action 1 [count()] With Cache: $cachedCount records in ${cachedTime} ms")
      val speedup = if (cachedTime > 0) f"${uncachedTime.toDouble / cachedTime}%.2fx" else "Immediate"
      println(s"         -> Performance Acceleration: $speedup faster access from in-memory cache!")

      // --------------------------------------------------------------------------
      // STEP 4: Key-Value Transformations & Actions
      // --------------------------------------------------------------------------
      println("\n[STEP 4] Key-Value Operations: Hotspot Analysis")

      // Key-Value Transformation: map() to (District, 1) and reduceByKey(_ + _)
      val districtCounts = crimesRdd
        .map(crime => (crime.district, 1))
        .reduceByKey(_ + _)

      // Transformation: sortBy() descending
      val sortedDistricts = districtCounts.sortBy(_._2, ascending = false)

      // Action: take(10)
      println("         -> Top 5 Crime Hotspot Districts (take action):")
      val topDistricts = sortedDistricts.take(5)
      topDistricts.zipWithIndex.foreach { case ((dist, count), idx) =>
        println(f"            ${idx + 1}. District $dist%-4s : $count%5d incidents")
      }

      // Key-Value Transformation: Crime Type Frequencies
      val crimeTypeCounts = crimesRdd
        .map(crime => (crime.primaryType, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)

      println("\n         -> Top 5 Primary Crime Categories (take action):")
      val topCrimes = crimeTypeCounts.take(5)
      topCrimes.zipWithIndex.foreach { case ((cType, count), idx) =>
        println(f"            ${idx + 1}. $cType%-25s : $count%5d incidents")
      }

      // Transformation 3: flatMap() - Tokenize location descriptions to extract location keywords
      println("\n         -> [Transformation 3: flatMap()] Tokenizing Location Keywords:")
      val locationKeywords = crimesRdd
        .flatMap(crime => crime.locationDescription.split("[\\s,/]+"))
        .filter(_.length > 2)
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)

      val topLocations = locationKeywords.take(5)
      topLocations.zipWithIndex.foreach { case ((loc, count), idx) =>
        println(f"            ${idx + 1}. Keyword '$loc%-12s' : $count%5d occurrences")
      }

      // Action: reduce() - Summing total incidents across partitions
      val totalIncidentsSum = districtCounts.map(_._2).reduce(_ + _)
      println(s"\n         -> Action [reduce()]: Verified total incident sum = $totalIncidentsSum")

      // Action: collect() - Collect arrest summary
      val arrestCounts = crimesRdd
        .map(c => (if (c.arrest) "Arrested (Cleared)" else "Unapprehended (Open)", 1))
        .reduceByKey(_ + _)
        .collect()

      println("         -> Action [collect()]: Arrest Clearance Distribution:")
      arrestCounts.foreach { case (status, count) =>
        val pct = (count.toDouble / cachedCount) * 100.0
        println(f"            * $status%-25s : $count%5d ($pct%.2f%%)")
      }

      // --------------------------------------------------------------------------
      // STEP 5: Spark Concepts: Partitions, Lineage & DAG
      // --------------------------------------------------------------------------
      println("\n[STEP 5] Spark Core Concepts: Partitions, Lineage, and DAG")
      println("         -> Current Partitions count: " + sortedDistricts.getNumPartitions)

      // Repartitioning Demonstration
      val repartitionedRdd = sortedDistricts.repartition(4)
      println(s"         -> After repartition(4): ${repartitionedRdd.getNumPartitions} partitions")

      val coalescedRdd = repartitionedRdd.coalesce(2)
      println(s"         -> After coalesce(2): ${coalescedRdd.getNumPartitions} partitions")

      // Lineage & DAG: toDebugString
      println("\n         -> RDD Lineage Graph (sortedDistricts.toDebugString):")
      println("--------------------------------------------------------------------------------")
      println(sortedDistricts.toDebugString)
      println("--------------------------------------------------------------------------------")
      println("         Lineage Explanation:")
      println("         - Indentations denote Shuffle Boundaries separating Execution Stages.")
      println("         - Stage 1: textFile -> MapPartitionsRDD (parse) -> MapPartitionsRDD (pair)")
      println("         - Stage 2: ShuffledRDD (reduceByKey) -> MapPartitionsRDD (swap/key)")
      println("         - Stage 3: ShuffledRDD (sortBy) -> MapPartitionsRDD (result)")
      println("         - Lineage ensures deterministic recomputation and fault tolerance.")

      println("\n================================================================================")
      println(" [SUCCESS] All Scala + Spark RDD Operations Successfully Completed!")
      println("================================================================================")

    } finally {
      sc.stop()
    }
  }
}
