#!/bin/bash

# Vérification du nombre d'arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <start_date> <end_date>"
    echo "Les dates doivent être au format YYYY-MM-DD"
    exit 1
fi

# Dates de début et de fin fournies par l'utilisateur
START_DATE=$1
END_DATE=$2

# Script Spark à exécuter
SPARK_SCRIPT="dev_spark/data_processing.py"

# Convertir les dates en secondes depuis l'époque pour la comparaison
START_SEC=$(date -d "$START_DATE" +%s)
END_SEC=$(date -d "$END_DATE" +%s)

# Boucle sur chaque jour
while [ "$START_SEC" -le "$END_SEC" ]; do
    # Formatage de la date actuelle
    CURRENT_DATE=$(date -d @$START_SEC +%Y-%m-%d)

    # Premier intervalle de temps de la journée (00h00m00s à 11h45m00s)
    START_TIME_1="$CURRENT_DATE 00:00:00"
    END_TIME_1="$CURRENT_DATE 11:45:00"

    # Second intervalle de temps de la journée (12h00m00s à 23h45m00s)
    START_TIME_2="$CURRENT_DATE 12:00:00"
    END_TIME_2="$CURRENT_DATE 23:45:00"

    echo "Traitement des données pour la première moitié du jour : $START_TIME_1 à $END_TIME_1"
    spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-ffi:2.2.7,com.github.jnr:jnr-posix:3.1.4 $SPARK_SCRIPT "$START_TIME_1" "$END_TIME_1"

    echo "Traitement des données pour la seconde moitié du jour : $START_TIME_2 à $END_TIME_2"
    spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-ffi:2.2.7,com.github.jnr:jnr-posix:3.1.4 $SPARK_SCRIPT "$START_TIME_2" "$END_TIME_2"

    # Aller au jour suivant
    START_SEC=$(( $START_SEC + 86400 )) # Ajouter 86400 secondes (1 jour)
done
