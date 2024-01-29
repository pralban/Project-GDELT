from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("GDELT Project Request 2") \
    .master("spark://tp-hadoop-33:7077") \
    .config("spark.cassandra.connection.host", "tp-hadoop-33") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Chargement des données depuis Cassandra
request_2 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_1_2", keyspace="gdelt") \
    .load()


def query_most_mentioned_events_by_country_and_date(country, granularity='year'):
    # Filtrer par pays
    filtered_df = request_2.filter(request_2.event_country == country)

    # Définir la fenêtre de partitionnement et de tri selon la granularité
    if granularity == 'year':
        window = Window.partitionBy("event_year").orderBy(F.desc("num_mentions"))
        final_df = filtered_df.withColumn("rank", F.row_number().over(window)) \
            .filter(F.col("rank") == 1) \
            .select("event_year", "event_country", "global_event_id", "num_mentions") \
            .orderBy(F.desc("event_year"))

    elif granularity == 'month':
        window = Window.partitionBy("event_year", "event_month").orderBy(F.desc("num_mentions"))
        final_df = filtered_df.withColumn("rank", F.row_number().over(window)) \
            .filter(F.col("rank") == 1) \
            .select("event_year", "event_month", "event_country", "global_event_id", "num_mentions") \
            .orderBy(F.desc("event_year"), F.desc("event_month"))

    elif granularity == 'day':
        window = Window.partitionBy("event_year", "event_month", "event_day").orderBy(F.desc("num_mentions"))
        final_df = filtered_df.withColumn("rank", F.row_number().over(window)) \
            .filter(F.col("rank") == 1) \
            .select("event_year", "event_month", "event_day", "event_country", "global_event_id", "num_mentions") \
            .orderBy(F.desc("event_year"), F.desc("event_month"), F.desc("event_day"))

    return final_df


# Request 2 : agrégation par année
start_time = time.time()
result_year = query_most_mentioned_events_by_country_and_date('FR', 'year')
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 2 (agrégation par année) : ",
      end_time - start_time)

# Request 2 : agrégation par mois
start_time = time.time()
result_month = query_most_mentioned_events_by_country_and_date('FR', 'month')
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 2 (agrégation par mois) : ",
      end_time - start_time)

# Request 2 : agrégation par jour
start_time = time.time()
result_day = query_most_mentioned_events_by_country_and_date('FR', 'day')
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 2 (agrégation par jour) : ",
      end_time - start_time)

spark.stop()
