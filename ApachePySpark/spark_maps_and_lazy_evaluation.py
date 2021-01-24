import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[4]').appName('maps_and_lazy_evaluation_example').getOrCreate()
sc = spark.sparkContext

log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# parallelize the log_of_songs to use with Spark
distributed_song_log = sc.parallelize(log_of_songs)

rdd = distributed_song_log.map(lambda song: song.lower()) # lazy evaluation
rdd.foreach(print)
