from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("GDELT Project Request 3") \
    .master("spark://tp-hadoop-33:7077") \
    .config("spark.cassandra.connection.host", "tp-hadoop-33") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# ___________________________REQUETE 3.1___________________________

request_31 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_all_by_source_and_date(source, granularity='year', limit=100):
    filtered_df = request_31.filter(request_31.internet_source == source)

    # Define sorting and select columns based on granularity
    if granularity == 'year':
        sorted_df = filtered_df.orderBy(F.desc("event_year"))
        selected_cols = ["internet_source", "event_year",
                         "event_themes", "person_list", "first_country", "event_tone"]
    elif granularity == 'month':
        sorted_df = filtered_df.orderBy(
            F.desc("event_year"), F.desc("event_month"))
        selected_cols = ["internet_source", "event_year", "event_month",
                         "event_themes", "person_list", "first_country", "event_tone"]
    elif granularity == 'day':
        sorted_df = filtered_df.orderBy(
            F.desc("event_year"), F.desc("event_month"), F.desc("event_day"))
        selected_cols = ["internet_source", "event_year", "event_month",
                         "event_day", "event_themes", "person_list", "first_country", "event_tone"]

    # Select the necessary columns and apply the limit
    return sorted_df.select(*selected_cols).limit(limit)


# Requête 3.1 : agrégation par année
start_time = time.time()
result_year = query_all_by_source_and_date('msn.com', 'year', limit=100)
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.1 (agrégation par année): ",
      end_time - start_time)

# Requête 3.1 : agrégation par mois
start_time = time.time()
result_month = query_all_by_source_and_date('msn.com', 'month', limit=100)
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.1 (agrégation par mois): ",
      end_time - start_time)

# Requête 3.1 : agrégation par jour
start_time = time.time()
result_day = query_all_by_source_and_date('msn.com', 'day', limit=100)
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.1 (agrégation par jour): ",
      end_time - start_time)


# ___________________________REQUETE 3.2___________________________

request_32 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_avg_tone_and_count_by_source_and_date(source, granularity='year'):
    # Filtrage initial par source Internet
    filtered_df = request_32.filter(request_32.internet_source == source)

    # Aggrégation en fonction de la granularité choisie
    if granularity == 'year':
        grouped_df = filtered_df.groupBy('internet_source', 'event_year').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count('global_event_id').alias('article_count')
        ).orderBy(F.desc('event_year'))
    elif granularity == 'month':
        grouped_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count('global_event_id').alias('article_count')
        ).orderBy(F.desc('event_year'), F.desc('event_month'))
    elif granularity == 'day':
        grouped_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month', 'event_day').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count('global_event_id').alias('article_count')
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('event_day'))

    return grouped_df


# Requête 3.2 : agrégation par année
start_time = time.time()
result_year = query_avg_tone_and_count_by_source_and_date(
    'msn.com', 'year')
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.2 (agrégation par année): ",
      end_time - start_time)

# Requête 3.2 : agrégation par mois
start_time = time.time()
result_month = query_avg_tone_and_count_by_source_and_date(
    'msn.com', 'month')
result_month.show(12)
end_time = time.time()
print("Temps d'exécution de la requête 3.2 (agrégation par mois): ",
      end_time - start_time)

# Requête 3.2 : agrégation par jour
start_time = time.time()
result_day = query_avg_tone_and_count_by_source_and_date('msn.com', 'day')
result_day.show(20)
end_time = time.time()
print("Temps d'exécution de la requête 3.2 (agrégation par jour): ",
      end_time - start_time)


# ___________________________REQUETE 3.3___________________________

request_33 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_themes_by_source_and_date(source, granularity='year', limit=3):
    filtered_df = request_33.filter(request_33.internet_source == source)

    # Perform grouping, aggregation, and ranking based on the specified granularity
    if granularity == 'year':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'event_themes').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('theme_count')
        )
        window = Window.partitionBy(
            'event_year').orderBy(F.desc('theme_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_themes', 'average_tone', 'theme_count'
        ).orderBy(F.desc('event_year'), F.desc('theme_count'))

    elif granularity == 'month':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month', 'event_themes').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('theme_count')
        )
        window = Window.partitionBy(
            'event_year', 'event_month').orderBy(F.desc('theme_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'event_themes', 'average_tone', 'theme_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('theme_count'))

    elif granularity == 'day':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month', 'event_day', 'event_themes').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('theme_count')
        )
        window = Window.partitionBy(
            'event_year', 'event_month', 'event_day').orderBy(F.desc('theme_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'event_day', 'event_themes', 'average_tone', 'theme_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('event_day'), F.desc('theme_count'))

    return final_df


# Requête 3.3 : agrégation par année
start_time = time.time()
result_year = query_themes_by_source_and_date('msn.com', 'year', limit=3)
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.3 (agrégation par année): ",
      end_time - start_time)

# Requête 3.3 : agrégation par mois
start_time = time.time()
result_month = query_themes_by_source_and_date(
    'msn.com', 'month', limit=3)
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.3 (agrégation par mois): ",
      end_time - start_time)

# Requête 3.3 : agrégation par jour
start_time = time.time()
result_day = query_themes_by_source_and_date('msn.com', 'day', limit=1)
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.3 (agrégation par jour): ",
      end_time - start_time)


# ___________________________REQUETE 3.4___________________________

request_34 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_locations_by_source_and_date(source, granularity='year', limit=3):
    filtered_df = request_34.filter(request_34.internet_source == source)

    # Perform grouping, aggregation, and ranking based on the specified granularity
    if granularity == 'year':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'first_country').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('location_count')
        )
        window = Window.partitionBy('event_year').orderBy(
            F.desc('location_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'first_country', 'average_tone', 'location_count'
        ).orderBy(F.desc('event_year'), F.desc('location_count'))

    elif granularity == 'month':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month', 'first_country').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('location_count')
        )
        window = Window.partitionBy('event_year', 'event_month').orderBy(
            F.desc('location_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'first_country', 'average_tone', 'location_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('location_count'))

    elif granularity == 'day':
        aggregated_df = filtered_df.groupBy('internet_source', 'event_year', 'event_month', 'event_day', 'first_country').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('location_count')
        )
        window = Window.partitionBy('event_year', 'event_month', 'event_day').orderBy(
            F.desc('location_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'event_day', 'first_country', 'average_tone', 'location_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('event_day'), F.desc('location_count'))

    return final_df


# Requête 3.4 : agrégation par année
start_time = time.time()
result_year = query_locations_by_source_and_date(
    'msn.com', 'year', limit=3)
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.4 (agrégation par année): ",
      end_time - start_time)

# Requête 3.4 : agrégation par mois
start_time = time.time()
result_month = query_locations_by_source_and_date(
    'msn.com', 'month', limit=3)
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.4 (agrégation par mois): ",
      end_time - start_time)

# Requête 3.4 : agrégation par jour
start_time = time.time()
result_day = query_locations_by_source_and_date('msn.com', 'day', limit=1)
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.4 (agrégation par jour): ",
      end_time - start_time)


# ___________________________REQUETE 3.5___________________________

request_35 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_persons_by_source_and_date(source, granularity='year', limit=3):
    filtered_df = request_35.filter(request_35.internet_source == source)

    # Explode the person list to get individual persons for counting
    exploded_df = filtered_df.withColumn(
        "person", F.explode(F.split(F.col("person_list"), ";")))

    # Perform grouping, aggregation, and ranking based on the specified granularity
    if granularity == 'year':
        aggregated_df = exploded_df.groupBy('internet_source', 'event_year', 'person').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('person_count')
        )
        window = Window.partitionBy(
            'event_year').orderBy(F.desc('person_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'person', 'average_tone', 'person_count'
        ).orderBy(F.desc('event_year'), F.desc('person_count'))

    elif granularity == 'month':
        aggregated_df = exploded_df.groupBy('internet_source', 'event_year', 'event_month', 'person').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('person_count')
        )
        window = Window.partitionBy(
            'event_year', 'event_month').orderBy(F.desc('person_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'person', 'average_tone', 'person_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('person_count'))

    elif granularity == 'day':
        aggregated_df = exploded_df.groupBy('internet_source', 'event_year', 'event_month', 'event_day', 'person').agg(
            F.avg('event_tone').alias('average_tone'),
            F.count("*").alias('person_count')
        )
        window = Window.partitionBy(
            'event_year', 'event_month', 'event_day').orderBy(F.desc('person_count'))
        ordered_df = aggregated_df.withColumn(
            'rank', F.row_number().over(window))
        final_df = ordered_df.filter(ordered_df['rank'] <= limit).select(
            'internet_source', 'event_year', 'event_month', 'event_day', 'person', 'average_tone', 'person_count'
        ).orderBy(F.desc('event_year'), F.desc('event_month'), F.desc('event_day'), F.desc('person_count'))

    return final_df


# Requête 3.5 : agrégation par année
start_time = time.time()
result_year = query_persons_by_source_and_date(
    'msn.com', 'year', limit=10)
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.5 (agrégation par année): ",
      end_time - start_time)

# Requête 3.5 : agrégation par mois
start_time = time.time()
result_month = query_persons_by_source_and_date(
    'msn.com', 'month', limit=3)
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.5 (agrégation par mois): ",
      end_time - start_time)

# Requête 3.5 : agrégation par jour
start_time = time.time()
result_day = query_persons_by_source_and_date('msn.com', 'day', limit=1)
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 3.5 (agrégation par jour): ",
      end_time - start_time)

spark.stop()
