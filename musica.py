from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("music_dataset")\
        .getOrCreate()

    print("Reading dataset.csv ...")
    path_music = "dataset.csv"
    df_music = spark.read.csv(path_music, header=True, inferSchema=True)
    
    # Renombrar columnas si es necesario
    df_music = df_music.withColumnRenamed("track_name", "title")
    df_music.createOrReplaceTempView("music")
    
    # Descripción de la tabla
    query = 'DESCRIBE music'
    spark.sql(query).show(20)

    # Filtrar canciones populares con un umbral de popularidad alto (ejemplo: > 80)
    query = """
    SELECT track_id, artists, album_name, title, popularity 
    FROM music WHERE popularity > 80 
    ORDER BY popularity DESC
    """
    df_popular_tracks = spark.sql(query)
    df_popular_tracks.show(20)

    # Filtrar canciones de un género específico, por ejemplo, 'acoustic'
    query = """
    SELECT track_id, artists, title, track_genre, tempo 
    FROM music WHERE track_genre = 'acoustic' 
    ORDER BY tempo DESC
    """
    df_acoustic_tracks = spark.sql(query)
    df_acoustic_tracks.show(20)

    # Guardar resultados en JSON
    results = df_acoustic_tracks.toJSON().collect()
    df_acoustic_tracks.write.mode("overwrite").json("results")
    
    with open('results/acoustic_tracks.json', 'w') as file:
        json.dump(results, file)

    # Contar canciones por género
    query = "SELECT track_genre, COUNT(track_genre) FROM music GROUP BY track_genre"

# Segundo resultado: canciones explícitas y con alta energía (> 0.8)
    query = """
    SELECT track_id, artists, title, energy, explicit 
    FROM music 
    WHERE explicit = true AND energy > 0.8 
    ORDER BY energy DESC
    """
    df_explicit_energy = spark.sql(query)
    df_explicit_energy.show(20)

    # Guardar este segundo resultado en un archivo JSON distinto
    results_explicit = df_explicit_energy.toJSON().collect()
    df_explicit_energy.write.mode("overwrite").json("results_explicit")
    
    with open('results/explicit_energy_tracks.json', 'w') as file:
        json.dump(results_explicit, file)
    df_genre_count = spark.sql(query)
    df_genre_count.show()
    
    spark.stop()
