from pyspark.sql import SparkSession
import time

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("GDELT Project Request 1") \
    .master("spark://tp-hadoop-33:7077") \
    .config("spark.cassandra.connection.host", "tp-hadoop-33") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Chargement des données depuis Cassandra
request_1 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="request_1_2", keyspace="gdelt") \
    .load()


def query_articles_by_date_country_and_optional_language(year, month, day, country, language=None):
    # Filtrage par date et pays
    filtered_df = request_1.filter((request_1.event_year == year) &
                                   (request_1.event_month == month) &
                                   (request_1.event_day == day) &
                                   (request_1.event_country == country))

    if language is not None:
        # Si la langue est spécifiée, filtrer les articles pour le triplet spécifique et compter
        return filtered_df.filter(request_1.source_language == language).groupBy('event_year', 'event_month', 'event_day', 'event_country', 'source_language').count()
    else:
        # Sinon, compter les articles par langue pour la date et le pays donnés
        return filtered_df.groupBy('event_year', 'event_month', 'event_day', 'event_country', 'source_language').count().orderBy('source_language')


# Requête 1 : agrégation par jour avec langue spécifiée
start_time = time.time()
result_1 = query_articles_by_date_country_and_optional_language(
    2022, 1, 20, 'FR', 'eng')
result_1.show()
end_time = time.time()
print("Temps d'exécution de la requête 1.1 : ", end_time - start_time)

# Requête 1 : agrégation par jour sans langue spécifiée
start_time = time.time()
result_2 = query_articles_by_date_country_and_optional_language(
    2022, 1, 20, 'FR')
result_2.show()
end_time = time.time()
print("Temps d'exécution de la requête 1.2 : ", end_time - start_time)

spark.stop()
