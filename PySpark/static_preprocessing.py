from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import col, trim, regexp_replace, to_date, to_timestamp, when, length

spark = SparkSession.builder.appName('Preprocess Historic Data').getOrCreate()

raw_df = spark.read.text('gs://hsitoric_data/histroicdata/historic_data.csv')

# Cleaing of unwanted symbols
cleaned_text_df = raw_df.withColumn("value", regexp_replace("value", r'(^|[^,])"([^,]|$)', r'\1\2'))
cleaned_text_df.write.mode("overwrite").text("gs://hsitoric_data/intermediate/cleaned_raw_text")
df = spark.read.csv(
    'gs://hsitoric_data/intermediate/cleaned_raw_text',
                    header=True, 
)

df_cleaned = df \
    .withColumn("Disc Number", regexp_replace(col("Disc Number"), "[^0-9]", "").cast("int")) \
    .withColumn("Track Number", regexp_replace(col("Track Number"), "[^0-9]", "").cast("int")) \
    .withColumn("Track Duration (ms)", regexp_replace(col("Track Duration (ms)"), "[^0-9.]", "").cast("float")) \
    .withColumn("Popularity", regexp_replace(col("Popularity"), "[^0-9]", "").cast("int")) \
    .withColumn("Explicit", when(col("Explicit") == "true", True)
                             .when(col("Explicit") == "false", False)
                             .otherwise(None).cast("boolean")) \
    .withColumn("Added At", to_timestamp(col("Added At")))\
    .withColumn("Genre", explode(split("Artist Genres", ","))) \
    .withColumn("Genre", trim("Genre"))

# Prepocessing
df_cleaned = df_cleaned.withColumn("duration_min", col("Track Duration (ms)").cast("float") / 60000)
df_cleaned = df_cleaned.withColumn(
    "Album Release Date",
    when(length(col("Album Release Date")) == 4, col("Album Release Date") + "-01-01")  
    .when(length(col("Album Release Date")) == 7, col("Album Release Date") + "-01")    
    .otherwise(col("Album Release Date"))                                          
)


# Renaming columns and setting the datatypes
df_renamed = df_cleaned.select(
    trim(col("Track URI")).alias("track_uri"),
    trim(col("Track Name")).alias("track_name"),
    trim(col("Artist URI(s)")).alias("artist_uri"),
    trim(col("Artist Name(s)")).alias("artist_names"),
    trim(col("Album URI")).alias("album_uri"),
    trim(col("Album Name")).alias("album_name"),
    trim(col("Album Release Date")).alias("release_date"),
    trim(col("Disc Number")).alias("disk_number"),
    trim(col("Track Number")).alias("track_number"),
    col("duration_min"),
    trim(col("Explicit")).alias("explicit"),
    trim(col("Popularity")).alias("popularity"),
    trim(col("ISRC")).alias("isrc"),
    trim(col("Added By")).alias("added_by"),
    trim(col("Added At")).alias("added_at"),
    trim(col("Artist Genres")).alias("artist_genres"),
    trim(col("Label")).alias("label"),
    trim(col("Copyrights")).alias("copyrights"),
)

df_renamed = df_renamed.select(
    col("track_uri"),
    col("track_name"),
    col("artist_uri"),
    col("artist_names"),
    col("album_uri"),
    col("album_name"),
    to_date("release_date", "yyyy-MM-dd").alias("release_date"),         
    col("disk_number").cast("int").alias("disk_number"),                 
    col("track_number").cast("int").alias("track_number"),               
    col("duration_min").cast("float").alias("duration_min"),                     
    col("explicit").cast("boolean").alias("explicit"),                   
    col("popularity").cast("int").alias("popularity"),                   
    col("isrc"),
    col("added_by"),
    to_timestamp("added_at").alias("added_at"),                          
    col("artist_genres"),
    col("label"),
    col("copyrights")
)

df_cleaned = df.dropDuplicates()

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

df_renamed.write \
    .format("Parquet") \
    .mode("overwrite") \
    .save("gs://hsitoric_data/cleandata")

spark.stop()
