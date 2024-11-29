from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import IntegerType, DoubleType


def consecutive_seasons(seasons, cutoff_points):
    count = 0
    max_consecutive_seasons = 0
    for season in seasons:
        if season.pts > cutoff_points:
            count += 1
        else:
            if count > max_consecutive_seasons:
                max_consecutive_seasons = count
            count = 0

    if count > max_consecutive_seasons:
        max_consecutive_seasons = count

    return max_consecutive_seasons


def biggest_point_delta(seasons):
    prev_season = None
    this_season = None
    point_delta = 0
    for season in seasons:
        if prev_season is None:
            prev_season = season.pts
        else:
            this_season = season.pts
            if(abs(this_season - prev_season) > point_delta):
                point_delta = abs(this_season - prev_season)
            prev_season = this_season
    return point_delta


def average_point_delta(seasons):
    prev_season = None
    this_season = None
    point_delta = 0
    season_count = 0
    for season in seasons:
        if prev_season is None:
            prev_season = season.pts
        else:
            this_season = season.pts
            point_delta += abs(this_season - prev_season)
            prev_season = this_season
        season_count += 1
    return point_delta / season_count


# Register the UDF
# Specify the return type (IntegerType() in this case)
consecutive_seasons_udf = udf(consecutive_seasons, IntegerType())
biggest_point_delta_udf = udf(biggest_point_delta, DoubleType())
average_point_delta_udf = udf(average_point_delta, DoubleType())

import os


def main():
    schema = 'zachwilson'
    processing_season = 2002
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

    df = spark.sql(f"""SELECT * FROM {schema}.nba_players WHERE current_season = {processing_season}""") \
        .withColumn("max_consecutive_20pt_seasons", consecutive_seasons_udf(col("seasons"), lit(20))) \
        .withColumn("max_consecutive_10pt_seasons", consecutive_seasons_udf(col("seasons"), lit(10))) \
        .withColumn("biggest_season_point_delta", biggest_point_delta_udf(col("seasons"))) \
        .withColumn("average_point_delta", average_point_delta_udf(col("seasons"))) \
        .sort(lit(-1) * col("max_consecutive_20pt_seasons"))

    output_table = f"{schema}.nba_players_deltas"
    output_ddl = f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            player_name STRING,
            max_consecutive_20pt_seasons INTEGER,
            max_consecutive_10pt_seasons INTEGER,
            biggest_season_point_delta DOUBLE,
            average_point_delta DOUBLE,
            current_season INTEGER
        )
        PARTITIONED BY (current_season)
    """

    spark.sql(output_ddl)

    df.select(col("player_name"),
              col("max_consecutive_20pt_seasons"),
              col("max_consecutive_10pt_seasons"),
              col("biggest_season_point_delta"),
              col("average_point_delta"),
              col("current_season")
              ).write.mode("overwrite").format("parquet").insertInto(output_table)

    spark.stop()

if __name__ == "__main__":
    main()