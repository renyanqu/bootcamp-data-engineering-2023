package com.dataengineer.io.examples

import com.dataengineer.io.examples.NbaPlayerTransformationJob.aggregateNbaPlayerDataframe
import com.dataengineer.io.examples.schema.{AggregatedNbaPlayerStatistics, NbaPlayer, Season}
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.BlockJUnit4ClassRunner
import org.scalatest.funsuite.AnyFunSuite

@RunWith(classOf[BlockJUnit4ClassRunner])
class NbaPlayerTransformationJobTest extends AnyFunSuite {

  @Test
  def test() {
    val spark = SparkSession.builder()
      .master("local")
      .appName("udf testings")
      .getOrCreate()

    import spark.implicits._
    val inputDataSet = List(
      NbaPlayer(player_name = "Zach Wilson", seasons = List(
        Season(
          pts = 20.0,
          reb = 0.7,
          ast = 1.0,
          gp = 81,
          season = 2002,
          age = 29,
          weight = 155
        )
      ), current_season = 2002),
      NbaPlayer(player_name = "Michael Jordan", seasons = List(
        Season(
          pts = 21.0,
          reb = 0.7,
          ast = 1.0,
          gp = 81,
          season = 2002,
          age = 29,
          weight = 200
        ),
        Season(
          pts = 21.0,
          reb = 0.7,
          ast = 1.0,
          gp = 81,
          season = 2001,
          age = 29,
          weight = 205
        )
      ), current_season = 2002)
    )

    val ds = spark.sparkContext.parallelize(inputDataSet).toDS()
    val outputDS = aggregateNbaPlayerDataframe(spark, ds)

    val expectedOutputDs = List(
      AggregatedNbaPlayerStatistics(
        player_name = "Zach Wilson",
        statistics = Map(
          "consecutive_30_pt_seasons" -> 0.0,
          "consecutive_20_pt_seasons" -> 0.0,
          "consecutive_10_pt_seasons" -> 1.0,
          "consecutive_1_pt_seasons" -> 1.0,
          "oldest_age" -> 29.0,
          "youngest_age" -> 29.0,
          "heaviest_weight" -> 155.0,
          "lightest_weight" -> 155.0
        ),
        current_season = 2002
      ),
      AggregatedNbaPlayerStatistics(
        player_name = "Michael Jordan",
        statistics = Map(
          "consecutive_30_pt_seasons" -> 0.0,
          "consecutive_20_pt_seasons" -> 2.0,
          "consecutive_10_pt_seasons" -> 2.0,
          "consecutive_1_pt_seasons" -> 2.0,
          "oldest_age" -> 29.0,
          "youngest_age" -> 29.0,
          "heaviest_weight" -> 205.0,
          "lightest_weight" -> 200.0

        ),
        current_season = 2002
      )
    )

    val keysToCheck = List(
      "consecutive_30_pt_seasons" ,
      "consecutive_20_pt_seasons",
      "consecutive_10_pt_seasons",
      "consecutive_1_pt_seasons",
      "oldest_age",
      "youngest_age",
      "heaviest_weight",
      "lightest_weight"
    )
    outputDS.foreach((output)=> {
      val foundPlayer = expectedOutputDs.filter((f)=> f.player_name == output.player_name)
      assert(foundPlayer.length == 1)
      assert(foundPlayer(0).current_season == output.current_season)
      keysToCheck.foreach((key)=> {
        assert(foundPlayer(0).statistics.getOrElse(key, 0.0) == output.statistics.getOrElse(key, 0.0))
      })
    })
  }
}