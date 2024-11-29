package com.dataengineer.io.examples.udf

import com.dataengineer.io.examples.schema.Season
import org.apache.spark.sql.functions.udf

object NbaPlayerUdfs {
  def consecutive_seasons(seasons: List[Season], cutoff_points: Integer): Integer = {
    var count = 0
    var max_consecutive_seasons = 0
    seasons.foreach((season) => {
      if (season.pts > cutoff_points) {
        count += 1
      }
      else{
        count = 0
      }
      if (count > max_consecutive_seasons) {
        max_consecutive_seasons = count
      }
    })
    max_consecutive_seasons
  }

  //If you wrap in UDF you can call it directly in dataframe APIs
  val consecutive_seasons_udf = udf(consecutive_seasons _)
}
