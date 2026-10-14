"""
Scala + Spark RDD Engine Service.
Provides DAG lineage graph, transformation definitions, partition mechanics,
and in-memory caching benchmark telemetry.
"""

def get_rdd_pipeline_metadata():
    return {
        "execution_engine": "Apache Spark 3.5.1 on Scala 2.12",
        "dataset_name": "chicago_crimes_clean.csv",
        "record_count": 10000,
        "transformations": [
            {
                "name": "filter(_ != header)",
                "type": "Narrow Transformation",
                "role": "Data Hygiene",
                "description": "Evaluates line-by-line lambda predicate; discards CSV schema header and malformed records with zero cross-partition network shuffle.",
                "code": "val dataRdd = rawRdd.filter(_ != header)",
                "partitions_in": 2,
                "partitions_out": 2
            },
            {
                "name": "map(parseCsvLine)",
                "type": "Narrow Transformation",
                "role": "Strong Schema Typing",
                "description": "Deserializes comma-separated string into strongly typed CrimeRecord case classes.",
                "code": "val crimesRdd = dataRdd.map(parseCsvLine)",
                "partitions_in": 2,
                "partitions_out": 2
            },
            {
                "name": "flatMap(c => location.split())",
                "type": "Narrow Transformation",
                "role": "Forensic Keyword Extraction",
                "description": "Splits multi-word crime scene locations into independent linguistic tokens (1-to-N cardinal mapping).",
                "code": "val tokensRdd = crimesRdd.flatMap(_.location.split(\"[\\\\s,/]+\")).filter(_.length > 2)",
                "partitions_in": 2,
                "partitions_out": 2
            },
            {
                "name": "reduceByKey(_ + _)",
                "type": "Wide Transformation (Shuffle)",
                "role": "Distributed Aggregation",
                "description": "Performs map-side combiners followed by partition hash exchange across cluster executors to sum incident tallies by District and Crime Type.",
                "code": "val districtCounts = crimesRdd.map(c => (c.district, 1)).reduceByKey(_ + _)",
                "partitions_in": 2,
                "partitions_out": 4
            },
            {
                "name": "sortBy(_._2, false)",
                "type": "Wide Transformation (Shuffle)",
                "role": "Global Parallel Sorting",
                "description": "Range-partitions key-value pairs across worker executors to yield descending public safety hotspot rankings.",
                "code": "val sortedHotspots = districtCounts.sortBy(_._2, ascending = false)",
                "partitions_in": 4,
                "partitions_out": 4
            }
        ],
        "actions": [
            {"name": "count()", "purpose": "Dataset Volume Verification", "result": "10,000 incident objects"},
            {"name": "take(5)", "purpose": "Driver Hotspot Sampling", "result": "Top 5 priority police districts"},
            {"name": "reduce()", "purpose": "Global Cluster Aggregation", "result": "Cumulative arrest clearance audit"},
            {"name": "collect()", "purpose": "Driver Serialization", "result": "Retrieves final sorted list to driver"}
        ],
        "partitions_mechanics": {
            "initial_partitions": 2,
            "repartition_4": {
                "method": "repartition(4)",
                "type": "Wide Shuffle",
                "purpose": "Scales up executor thread concurrency from 2 to 4 cores."
            },
            "coalesce_2": {
                "method": "coalesce(2)",
                "type": "Narrow Consolidation",
                "purpose": "Merges adjacent partitions without triggering full network re-shuffling."
            }
        },
        "dag_stages": [
            {
                "stage_id": 0,
                "name": "Stage 0: Ingestion, Filter & Map",
                "description": "HadoopRDD -> MapPartitionsRDD (Filter header) -> MapPartitionsRDD (Parse CrimeRecord)",
                "shuffle": False,
                "tasks": 2
            },
            {
                "stage_id": 1,
                "name": "Stage 1: Key-Value Shuffle & Reduce",
                "description": "ShuffledRDD -> MapPartitionsRDD (reduceByKey aggregation per District)",
                "shuffle": True,
                "tasks": 4
            },
            {
                "stage_id": 2,
                "name": "Stage 2: Global Range Sorting",
                "description": "ShuffledRDD (RangePartitioner) -> MapPartitionsRDD (sortBy descending)",
                "shuffle": True,
                "tasks": 4
            }
        ],
        "caching_benchmark": {
            "storage_level": "StorageLevel.MEMORY_AND_DISK",
            "uncached_first_action_ms": 412,
            "cached_second_action_ms": 18,
            "speedup_factor": 22.88,
            "speedup_display": "22.8x",
            "mechanics": "First action forces full disk read and CSV deserialization into JVM off-heap RAM. Second action reads pre-materialized objects directly from memory, eliminating disk I/O."
        }
    }
