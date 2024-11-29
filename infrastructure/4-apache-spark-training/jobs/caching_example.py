from pyspark.sql import SparkSession

from pyspark.sql.functions import col

import os


def main():
    schema = 'zachwilson'
    if not os.environ['DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL'] or not os.environ['DATA_ENGINEER_IO_WAREHOUSE']:
        raise ValueError("""You need to set environment variables:
                    DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL, 
                    DATA_ENGINEER_IO_WAREHOUSE to run this PySpark job!
        """)

    # Initialize SparkConf and SparkContext
    spark = SparkSession.builder \
        .appName("PySparkSQLReadFromTable") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", os.environ['DATA_ENGINEER_IO_WAREHOUSE']) \
        .config("spark.sql.catalog.eczachly-academy-warehouse",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.eczachly-academy-warehouse.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.eczachly-academy-warehouse.uri",
                "https://api.tabular.io/ws/") \
        .config("spark.sql.catalog.eczachly-academy-warehouse.credential",
                os.environ['DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL']) \
        .config("spark.sql.catalog.eczachly-academy-warehouse.warehouse",
                os.environ['DATA_ENGINEER_IO_WAREHOUSE']) \
        .getOrCreate()

    popular_movie_metadata_df = spark.sql(f"""
        SELECT * FROM bootcamp.movie_metadata 
        WHERE TRY_CAST(popularity AS DOUBLE) > 1.0
    """).cache()
    movie_credits_df = spark.sql(f"""
        SELECT * FROM bootcamp.movie_credits
    """)
    actor_films_df = spark.sql(f"""
        SELECT * FROM bootcamp.actor_films
    """)

    actors_and_metadata_df = popular_movie_metadata_df.join(actor_films_df,
                                                    popular_movie_metadata_df["film_id"] == actor_films_df["film_id"]) \
        .select(
        popular_movie_metadata_df["*"],
        actor_films_df["actor"]
    ).sortWithinPartitions(col("actor"))

    credits_and_metadata_df = popular_movie_metadata_df \
                                .join(movie_credits_df,
                                      popular_movie_metadata_df["id"] == movie_credits_df["movie_id"]
                                )

    credits_and_metadata_df.write.mode("overwrite").format("parquet").saveAsTable(f"{schema}.metadata_with_credits")
    actors_and_metadata_df.write.mode("overwrite").format("parquet").saveAsTable(f"{schema}.metadata_with_actor")
    spark.stop()

if __name__ == "__main__":
    main()