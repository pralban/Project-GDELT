from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("GDELT Project Request 4") \
    .master("spark://tp-hadoop-33:7077") \
    .config("spark.cassandra.connection.host", "tp-hadoop-33") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# ___________________________REQUETE 4.1___________________________

request_41 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_tone_and_count_by_countries_and_date(country1, country2, granularity='year'):
    # Ensure country1 and country2 are in a consistent order
    filtered_df = request_41.withColumn(
        "sorted_country_pair",
        F.when(F.col("first_country") < F.col("second_country"),
               F.array("first_country", "second_country"))
        .otherwise(F.array("second_country", "first_country"))
    )

    # Filter the DataFrame for rows matching the specified country pair
    filtered_df = filtered_df.filter(
        (F.col("sorted_country_pair")[0] == country1) &
        (F.col("sorted_country_pair")[1] == country2)
    )

    # Group by the consistent country pair and aggregate based on granularity
    if granularity == 'year':
        aggregated_df = filtered_df.groupBy("sorted_country_pair", "event_year").agg(
            F.avg("event_tone").alias("average_tone"),
            F.count("*").alias("event_count")
        ).orderBy(F.desc("event_year"))
        # Select and rename the sorted country pair back to individual country columns for output
        final_df = aggregated_df.select(
            F.col("sorted_country_pair")[0].alias("first_country"),
            F.col("sorted_country_pair")[1].alias("second_country"),
            "event_year",
            "average_tone",
            "event_count"
        )

    elif granularity == 'month':
        aggregated_df = filtered_df.groupBy("sorted_country_pair", "event_year", "event_month").agg(
            F.avg("event_tone").alias("average_tone"),
            F.count("*").alias("event_count")
        ).orderBy(F.desc("event_year"), F.desc("event_month"))
        # Select and rename the sorted country pair back to individual country columns for output
        final_df = aggregated_df.select(
            F.col("sorted_country_pair")[0].alias("first_country"),
            F.col("sorted_country_pair")[1].alias("second_country"),
            "event_year",
            "event_month",
            "average_tone",
            "event_count"
        )

    elif granularity == 'day':
        aggregated_df = filtered_df.groupBy("sorted_country_pair", "event_year", "event_month", "event_day").agg(
            F.avg("event_tone").alias("average_tone"),
            F.count("*").alias("event_count")
        ).orderBy(F.desc("event_year"), F.desc("event_month"), F.desc("event_day"))
        # Select and rename the sorted country pair back to individual country columns for output
        final_df = aggregated_df.select(
            F.col("sorted_country_pair")[0].alias("first_country"),
            F.col("sorted_country_pair")[1].alias("second_country"),
            "event_year",
            "event_month",
            "event_day",
            "average_tone",
            "event_count"
        )

    return final_df


# Requête 4.1 : agrégation par année
start_time = time.time()
query_tone_and_count_by_countries_and_date(
    'France', 'United States', 'year').show()
end_time = time.time()
print("Temps d'exécution de la requête 4.1 (agrégation par année) : ",
      end_time - start_time)

# Requête 4.1 : agrégation par mois
start_time = time.time()
query_tone_and_count_by_countries_and_date(
    'France', 'United States', 'month').show()
end_time = time.time()
print("Temps d'exécution de la requête 4.1 (agrégation par mois) : ",
      end_time - start_time)

# Requête 4.1 : agrégation par jour
start_time = time.time()
query_tone_and_count_by_countries_and_date(
    'France', 'United States', 'day').show()
end_time = time.time()
print("Temps d'exécution de la requête 4.1 (agrégation par jour) : ",
      end_time - start_time)


# ___________________________REQUETE 4.2___________________________

request_42 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_3_4", keyspace="gdelt") \
    .load()


def query_themes_by_countries_and_date(country1, country2, granularity='year', limit=3):
    # Ensure country1 and country2 are in a consistent order and considered as a single entity
    when_condition = F.when(F.col("first_country") < F.col("second_country"),
                            F.concat_ws('-', F.col("first_country"), F.col("second_country"))) \
        .otherwise(F.concat_ws('-', F.col("second_country"), F.col("first_country")))

    filtered_df = request_42.withColumn("country_pair", when_condition)

    # Filter based on the consistent country pair
    filtered_df = filtered_df.filter(filtered_df.country_pair.contains(
        country1) & filtered_df.country_pair.contains(country2))

    # Perform grouping, aggregation, and ranking based on the specified granularity
    if granularity == 'year':
        window = Window.partitionBy(
            "event_year").orderBy(F.desc("theme_count"))
        grouped_df = filtered_df.groupBy(
            "country_pair", "event_year", "event_themes")
    elif granularity == 'month':
        window = Window.partitionBy(
            "event_year", "event_month").orderBy(F.desc("theme_count"))
        grouped_df = filtered_df.groupBy(
            "country_pair", "event_year", "event_month", "event_themes")
    elif granularity == 'day':
        window = Window.partitionBy(
            "event_year", "event_month", "event_day").orderBy(F.desc("theme_count"))
        grouped_df = filtered_df.groupBy(
            "country_pair", "event_year", "event_month", "event_day", "event_themes")

    # Aggregate the data and rank the themes
    aggregated_df = grouped_df.agg(
        F.avg("event_tone").alias("average_tone"),
        F.count("*").alias("theme_count")
    ).withColumn("rank", F.row_number().over(window))

    # Filter to the top 'limit' themes per granularity and select the output columns
    if granularity == 'year':
        final_df = aggregated_df.filter(aggregated_df['rank'] <= limit).select(
            F.split(F.col("country_pair"), '-')[0].alias("first_country"),
            F.split(F.col("country_pair"), '-')[1].alias("second_country"),
            "event_year", "event_themes", "average_tone", "theme_count"
        ).orderBy(F.desc("event_year"), F.desc("theme_count"))
    elif granularity == 'month':
        final_df = aggregated_df.filter(aggregated_df['rank'] <= limit).select(
            F.split(F.col("country_pair"), '-')[0].alias("first_country"),
            F.split(F.col("country_pair"), '-')[1].alias("second_country"),
            "event_year", "event_month", "event_themes", "average_tone", "theme_count"
        ).orderBy(F.desc("event_year"), F.desc("event_month"), F.desc("theme_count"))
    elif granularity == 'day':
        final_df = aggregated_df.filter(aggregated_df['rank'] <= limit).select(
            F.split(F.col("country_pair"), '-')[0].alias("first_country"),
            F.split(F.col("country_pair"), '-')[1].alias("second_country"),
            "event_year", "event_month", "event_day", "event_themes", "average_tone", "theme_count"
        ).orderBy(F.desc("event_year"), F.desc("event_month"), F.desc("event_day"), F.desc("theme_count"))

    return final_df


# Requête 4.2 : agrégation par année
start_time = time.time()
result_year = query_themes_by_countries_and_date(
    'France', 'United States', 'year', limit=3)
result_year.show()
end_time = time.time()
print("Temps d'exécution de la requête 4.2 (agrégation par année) : ",
      end_time - start_time)

# Requête 4.2 : agrégation par mois
start_time = time.time()
result_month = query_themes_by_countries_and_date(
    'France', 'United States', 'month', limit=3)
result_month.show()
end_time = time.time()
print("Temps d'exécution de la requête 4.2 (agrégation par mois) : ",
      end_time - start_time)

# Requête 4.2 : agrégation par jour
start_time = time.time()
result_day = query_themes_by_countries_and_date(
    'France', 'United States', 'day', limit=3)
result_day.show()
end_time = time.time()
print("Temps d'exécution de la requête 4.2 (agrégation par jour) : ",
      end_time - start_time)

spark.stop()
