// 23CSE352: Big Data Analytics - Project Review 3
// Interactive Spark-Shell Script for Scala + Spark RDD Operations
// To execute in spark-shell: :load src/main/scala/bigdata/spark_rdd_script.scala

import org.apache.spark.storage.StorageLevel

println("\n========================================================")
println(" Loading Chicago Crime Patterns RDD Dataset...")
println("========================================================")

val datasetPath = "dataset/chicago_crimes_clean.csv"
val rawRdd = sc.textFile(datasetPath)
println(s"-> Raw Partitions: ${rawRdd.getNumPartitions}")

val header = rawRdd.first()
val dataRdd = rawRdd.filter(line => line.nonEmpty && line != header)

// Transformations: map()
case class CrimeRec(district: String, primaryType: String, location: String, arrest: Boolean)

val parsedRdd = dataRdd.map { line =>
  val tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
  val pType = if (tokens.length > 5) tokens(5).replace("\"", "").trim.toUpperCase else "UNKNOWN"
  val loc = if (tokens.length > 7) tokens(7).replace("\"", "").trim.toUpperCase else "OTHER"
  val arr = if (tokens.length > 8) tokens(8).trim.equalsIgnoreCase("true") else false
  val distRaw = if (tokens.length > 11) tokens(11).trim else "000"
  val dist = if (distRaw.length == 1) "00" + distRaw else if (distRaw.length == 2) "0" + distRaw else distRaw
  CrimeRec(dist, pType, loc, arr)
}

// Caching benchmark
println("\n[Cache Benchmark]")
val t0 = System.currentTimeMillis()
val c1 = parsedRdd.count()
val t1 = System.currentTimeMillis()
println(s"-> Count without cache: $c1 in ${t1 - t0} ms")

parsedRdd.persist(StorageLevel.MEMORY_AND_DISK)
parsedRdd.count() // materialize

val t2 = System.currentTimeMillis()
val c2 = parsedRdd.count()
val t3 = System.currentTimeMillis()
println(s"-> Count with cache: $c2 in ${t3 - t2} ms (Accelerated)")

// Transformations: reduceByKey() & Actions: take(), collect()
println("\n[Top Crime Districts (reduceByKey + take)]")
val districtCounts = parsedRdd.map(c => (c.district, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
districtCounts.take(5).foreach { case (d, count) => println(f"   District $d : $count%5d incidents") }

println("\n[Top Crime Types (reduceByKey + take)]")
val crimeCounts = parsedRdd.map(c => (c.primaryType, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
crimeCounts.take(5).foreach { case (ct, count) => println(f"   $ct%-25s : $count%5d incidents") }

println("\n[Location Keywords (flatMap + reduceByKey)]")
val locKeywords = parsedRdd.flatMap(c => c.location.split("[\\s,/]+")).filter(_.length > 2).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
locKeywords.take(5).foreach { case (w, count) => println(f"   Keyword '$w%-12s' : $count%5d") }

println("\n[RDD Lineage & DAG (toDebugString)]")
println(districtCounts.toDebugString)
println("\n[SUCCESS] Interactive RDD execution complete!")
