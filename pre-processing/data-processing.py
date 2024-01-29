from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import col, when, to_date, year, month, dayofmonth, substring, split, udf
import requests
from io import BytesIO
from zipfile import ZipFile
import sys
import datetime

# Initialisation de Spark
conf = (SparkConf().setAppName("GDELT Project")
        .setMaster("spark://tp-hadoop-33:7077")
        .set("spark.executor.memory", "6g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# Configuration additionnelle pour la connexion à Cassandra
conf.set("spark.cassandra.connection.host", "tp-hadoop-33")
conf.set("spark.cassandra.connection.port", "9042")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# Fonction pour récupérer les URLs de fichier
def get_file_urls(base_url, start_date, end_date, file_type):
    response = requests.get(base_url)
    if response.status_code != 200:
        print(
            f"Erreur lors de l'accès à {base_url}: Statut {response.status_code}")
        return []

    lines = response.text.split('\n')
    file_urls = []
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and file_type in parts[2]:
            url = parts[2]
            # Extraire la date et l'heure de l'URL
            date_part = url.split('/')[-1].split('.')[0]
            try:
                file_date = datetime.datetime.strptime(
                    date_part, "%Y%m%d%H%M%S")
                if start_date <= file_date <= end_date:
                    file_urls.append(url)
            except ValueError:
                continue  # Ignorer les lignes avec des formats de date incorrects
    return file_urls

def download_and_process_in_memory(url):
    response = requests.get(url)
    zipfile = ZipFile(BytesIO(response.content))
    # Traitement des fichiers en mémoire
    rdd_list = []
    for filename in zipfile.namelist():
        with zipfile.open(filename) as file:
            content = file.read()
            try:
                # Essayez d'abord avec l'encodage utf-8
                lines = content.decode('utf-8').split('\n')
            except UnicodeDecodeError:
                # Si une erreur se produit, utilisez un encodage différent ou ignorez les erreurs
                lines = content.decode('iso-8859-1', errors='ignore').split('\n')
            rdd = sc.parallelize(lines)
            # Répartition des données pour améliorer l'efficacité de la gestion de la mémoire
            rdd = rdd.repartition(28)  # Ajustez ce nombre selon vos besoins
            rdd_list.append(rdd)
    return rdd_list

# Fonction pour convertir RDD en DataFrame
def rdd_to_df(rdd, schema):
    if not rdd.isEmpty():
        return spark.read.csv(rdd, schema=schema, sep="\t", header=False)
    return spark.createDataFrame([], schema)

# Définition des schémas
schema_gkg = StructType([
    StructField("GKGRECORDID", StringType(), True),
    StructField("DATE", LongType(), True),
    StructField("SourceCollectionIdentifier", IntegerType(), True),
    StructField("SourceCommonName", StringType(), True),
    StructField("DocumentIdentifier", StringType(), True),
    StructField("Counts", StringType(), True),
    StructField("V2Counts", StringType(), True),
    StructField("Themes", StringType(), True),
    StructField("V2Themes", StringType(), True),
    StructField("Locations", StringType(), True),
    StructField("V2Locations", StringType(), True),
    StructField("Persons", StringType(), True),
    StructField("V2Persons", StringType(), True),
    StructField("Organizations", StringType(), True),
    StructField("V2Organizations", StringType(), True),
    StructField("V2Tone", StringType(), True),
    StructField("Dates", StringType(), True),
    StructField("GCAM", StringType(), True),
    StructField("SharingImage", StringType(), True),
    StructField("RelatedImages", StringType(), True),
    StructField("SocialImageEmbeds", StringType(), True),
    StructField("SocialVideoEmbeds", StringType(), True),
    StructField("Quotations", StringType(), True),
    StructField("AllNames", StringType(), True),
    StructField("Amounts", StringType(), True),
    StructField("TranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
])

schema_mentions = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("EventTimeDate", LongType(), True),
    StructField("MentionTimeDate", LongType(), True),
    StructField("MentionType", IntegerType(), True),
    StructField("MentionSourceName", StringType(), True),
    StructField("MentionIdentifier", StringType(), True),
    StructField("SentenceID", IntegerType(), True),
    StructField("Actor1CharOffset", IntegerType(), True),
    StructField("Actor2CharOffset", IntegerType(), True),
    StructField("ActionCharOffset", IntegerType(), True),
    StructField("InRawText", IntegerType(), True),
    StructField("Confidence", IntegerType(), True),
    StructField("MentionDocLen", IntegerType(), True),
    StructField("MentionDocTone", DoubleType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
])

schema_export = StructType([
    StructField("GlobalEventID", LongType(), True),
    StructField("Day", IntegerType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", DoubleType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", DoubleType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", DoubleType(), True),
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", DoubleType(), True),
    StructField("Actor1Geo_Long", DoubleType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", IntegerType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", DoubleType(), True),
    StructField("Actor2Geo_Long", DoubleType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", DoubleType(), True),
    StructField("ActionGeo_Long", DoubleType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True)
])

# URLs de base pour les fichiers GDELT
base_urls = [
    "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
    "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
]

# Vérifiez si les dates de début et de fin sont fournies
if len(sys.argv) != 3:
    print("Usage: script.py <start_datetime> <end_datetime>")
    sys.exit(-1)

# Lire les dates de début et de fin à partir des arguments du script
# Les arguments sont attendus sous la forme "YYYY-MM-DD HH:MM:SS"
start_datetime_str, end_datetime_str = sys.argv[1], sys.argv[2]

# Conversion des chaînes en objets datetime
start_date = datetime.datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
end_date = datetime.datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')

# Récupération des URLs
file_urls = []
for base_url in base_urls:
    file_urls.extend(get_file_urls(
        base_url, start_date, end_date, 'export.CSV.zip'))
    file_urls.extend(get_file_urls(
        base_url, start_date, end_date, 'mentions.CSV.zip'))
    file_urls.extend(get_file_urls(
        base_url, start_date, end_date, 'gkg.csv.zip'))

# Traitement des données en mémoire
rdd_export, rdd_mentions, rdd_gkg = sc.emptyRDD(), sc.emptyRDD(), sc.emptyRDD()
for url in file_urls:
    rdd_list = download_and_process_in_memory(url)
    for rdd in rdd_list:
        if 'export.CSV' in url:
            rdd_export = rdd_export.union(rdd)
        elif 'mentions.CSV' in url:
            rdd_mentions = rdd_mentions.union(rdd)
        elif 'gkg.csv' in url:
            rdd_gkg = rdd_gkg.union(rdd)

# Chargement des données dans des DataFrames et affichage
df_export = rdd_to_df(rdd_export, schema_export)
df_mentions = rdd_to_df(rdd_mentions, schema_mentions)
df_mentions_export = df_export.join(df_mentions, "GlobalEventID", "inner")
df_gkg = rdd_to_df(rdd_gkg, schema_gkg)

# Convert the 'Day' column to a date format in export_mentions
df_mentions_export = df_mentions_export.withColumn(
    "Date", to_date(col("Day").cast("string"), 'yyyyMMdd'))

# Extract Year, Month, and Day columns from the 'Date' column
df_mentions_export = df_mentions_export.withColumn("Year", year("Date")) \
    .withColumn("Month", month("Date")) \
    .withColumn("Day", dayofmonth("Date"))

# Convert the numeric timestamp to a string
df_gkg = df_gkg.withColumn("DATE_NUMBER", col("DATE").cast("bigint"))

# Extract year, month, and day as separate columns
df_gkg = df_gkg.withColumn("Year", substring(
    col("DATE_NUMBER"), 1, 4).cast("int"))
df_gkg = df_gkg.withColumn("Month", substring(
    col("DATE_NUMBER"), 5, 2).cast("int"))
df_gkg = df_gkg.withColumn("Day", substring(
    col("DATE_NUMBER"), 7, 2).cast("int"))

df_gkg = df_gkg.drop("DATE_NUMBER")


# __________________PRETRAITEMENT REQUETES 1 ET 2_____________________

# Select necessary columns for request 1-2

# Select necessary columns for request 1
request_1_2_columns = ['GlobalEventID', 'Year', 'Month', 'Day', 'ActionGeo_CountryCode',
                       'ActionGeo_FullName', 'MentionDocTranslationInfo', 'NumMentions']  # Noms des colonnes à garder

# Select and rename necessary columns from each DataFrame
request_1_2 = df_mentions_export.select(*request_1_2_columns) \
    .withColumnRenamed("GlobalEventID", "global_event_id") \
    .withColumnRenamed("Year", "event_year") \
    .withColumnRenamed("Month", "event_month") \
    .withColumnRenamed("Day", "event_day") \
    .withColumnRenamed("ActionGeo_CountryCode", "event_country") \
    .withColumnRenamed("ActionGeo_FullName", "event_country_full_name") \
    .withColumnRenamed("MentionDocTranslationInfo", "source_language") \
    .withColumnRenamed("NumMentions", "num_mentions")

# Process MentionDocTranslationInfo column to extract source language
request_1_2 = request_1_2.withColumn(
    "source_language", col("source_language").substr(7, 3))
request_1_2 = request_1_2.withColumn("source_language", when(
    col("source_language") != "", col("source_language")).otherwise("eng"))

# Replace empty strings with null
request_1_2 = request_1_2.replace('', None)

# Drop rows where any of the columns are null
request_1_2 = request_1_2.dropna(how='any', subset=[
                                 'global_event_id', 'event_year', 'event_month', 'event_day', 'event_country'])

# __________________PRETRAITEMENT REQUETES 3 ET 4_____________________

def extract_countries_udf(location_info):
    # Split the location info by semicolon to separate countries
    country_segments = location_info.split(';')

    # Initialize an empty list to hold the country names
    country_names = []

    # Process each segment to extract the country name
    for segment in country_segments:
        if segment:
            # Split the segment by hash to get the blocks
            blocks = segment.split('#')
            # The country name is the last part of the second block (blocks[1])
            country_details = blocks[1] if len(blocks) > 1 else None
            if country_details:
                # Split the country details by comma and get the last element
                country_name = country_details.split(',')[-1].strip()
                country_names.append(country_name)

    # Get the first and second country names if they exist
    first_country = country_names[0] if len(country_names) > 0 else None
    second_country = country_names[1] if len(country_names) > 1 else None

    return [first_country, second_country]


def extract_first_tone_udf(event_tone):
    event_tone_str = str(event_tone)
    return event_tone_str.split(',')[0] if event_tone_str else None


def extract_first_theme_udf(event_themes):
    return event_themes.split('#')[0] if event_themes else None


# Select necessary columns for request 3-4
request_3_4_columns = ['GKGRECORDID', 'SourceCommonName', 'Year',
                       'Month', 'Day', 'V2Counts', 'Persons', 'Locations', 'V2Tone']

# Select columns first, then rename them
request_3_4 = df_gkg.select(*request_3_4_columns) \
    .withColumnRenamed("GKGRECORDID", "global_event_id") \
    .withColumnRenamed("SourceCommonName", "internet_source") \
    .withColumnRenamed("Year", "event_year") \
    .withColumnRenamed("Month", "event_month") \
    .withColumnRenamed("Day", "event_day") \
    .withColumnRenamed("V2Counts", "event_themes") \
    .withColumnRenamed("Persons", "person_list") \
    .withColumnRenamed("Locations", "countries_name") \
    .withColumn("event_tone", split(col("V2Tone"), ",")[0].cast("double")) \
    .drop('V2Tone')

# Replace empty strings with null
request_3_4 = request_3_4.na.replace('', None)

# Drop rows where any of the columns are null
request_3_4 = request_3_4.na.drop(how='any', subset=[
                                  'global_event_id', 'event_year', 'event_month', 'event_day', 'countries_name', 'event_themes', 'event_tone'])

# Register the UDFs with PySpark
spark_extract_countries_udf = udf(
    extract_countries_udf, ArrayType(StringType()))
spark_extract_first_tone_udf = udf(extract_first_tone_udf, StringType())
spark_extract_first_theme_udf = udf(extract_first_theme_udf, StringType())

# Apply the UDFs to the DataFrame
request_3_4 = request_3_4.withColumn(
    "countries", spark_extract_countries_udf(col("countries_name")))

# Select the first and second countries
request_3_4 = request_3_4.withColumn("first_country", col("countries")[0])
request_3_4 = request_3_4.withColumn("second_country", col("countries")[1])

# Extract the first theme and first tone
request_3_4 = request_3_4.withColumn(
    "event_themes", spark_extract_first_theme_udf(col("event_themes")))
request_3_4 = request_3_4.withColumn(
    "event_tone", spark_extract_first_tone_udf(col("event_tone")))

# Drop the 'Countries_name' and 'Countries' columns
request_3_4 = request_3_4.drop("countries_name", "countries")

request_1_2.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="request_1_2", keyspace="gdelt") \
    .save()

request_3_4.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="request_3_4", keyspace="gdelt") \
    .save()

spark.stop()
