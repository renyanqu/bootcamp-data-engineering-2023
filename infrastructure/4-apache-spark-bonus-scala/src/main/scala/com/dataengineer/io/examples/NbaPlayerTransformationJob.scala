package com.dataengineer.io.examples

import com.dataengineer.io.examples.schema.{AggregatedNbaPlayerStatistics, NbaPlayer, Season, SlimSeason}
import com.dataengineer.io.examples.udf.NbaPlayerUdfs.consecutive_seasons
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

object NbaPlayerTransformationJob {


  val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)


  def aggregateNbaPlayerDataframe(spark: SparkSession, dataset: Dataset[NbaPlayer]): Dataset[AggregatedNbaPlayerStatistics] = {
    import spark.implicits._

    dataset.map(row => {
      val weightArray = row.seasons.filter(r => r.weight != null)


      val heaviestWeight = if (weightArray.nonEmpty) weightArray.map((r) => r.weight).max.toDouble else 0.0
      val lightestWeight = if (weightArray.nonEmpty) weightArray.map((r) => r.weight).min.toDouble else 0.0
      AggregatedNbaPlayerStatistics(
        player_name = row.player_name,
        statistics = Map(
          "consecutive_30_pt_seasons" -> consecutive_seasons(row.seasons, 30).toDouble,
          "consecutive_20_pt_seasons" -> consecutive_seasons(row.seasons, 20).toDouble,
          "consecutive_10_pt_seasons" -> consecutive_seasons(row.seasons, 10).toDouble,
          "consecutive_1_pt_seasons" -> consecutive_seasons(row.seasons, 1).toDouble,
          "oldest_age" -> row.seasons.map((r) => r.age).max.toDouble,
          "youngest_age" -> row.seasons.map((r) => r.age).min.toDouble,
          "heaviest_weight" -> heaviestWeight,
          "lightest_weight" -> lightestWeight
        ),
        current_season = row.current_season
      )
    })
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("DataSetAPIExample")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.defaultCatalog", scala.util.Properties.envOrElse("DATA_ENGINEER_IO_WAREHOUSE", ""))
      .config("spark.sql.catalog.eczachly-academy-warehouse", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.eczachly-academy-warehouse.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      .config("spark.sql.catalog.eczachly-academy-warehouse.uri", "https://api.tabular.io/ws/")
      .config("spark.sql.catalog.eczachly-academy-warehouse.credential",
        scala.util.Properties.envOrElse("DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL", ""))
      .config("spark.sql.catalog.eczachly-academy-warehouse.warehouse",
        scala.util.Properties.envOrElse("DATA_ENGINEER_IO_WAREHOUSE", ""))
      .getOrCreate()

    import spark.implicits._

    val data = spark.sql("SELECT * FROM bootcamp.nba_players WHERE current_season = 2002").as[NbaPlayer]

    val mapped = aggregateNbaPlayerDataframe(spark, data)

    mapped.write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("current_season")
      .saveAsTable("bootcamp.nba_player_aggregate_statistics")

    LOG.info("NUMBER OF RECORDS:" + mapped.collect().length)
    LOG.info("Dummy info message")
    LOG.warn("Dummy warn message")
    LOG.error("Dummy error message")
  }

}
