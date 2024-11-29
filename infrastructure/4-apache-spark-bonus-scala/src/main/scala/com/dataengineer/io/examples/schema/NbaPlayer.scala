package com.dataengineer.io.examples.schema

case class Season(
                   age: Integer,
                   season: Integer,
                   weight: Integer,
                   gp: Integer,
                   pts: Double,
                   reb: Double,
                   ast: Double
                 )


case class SlimSeason(
                     pts: Double,
                     ast: Double
                     )

case class NbaPlayer(
                      player_name: String,
                      seasons: List[Season],
                      current_season: Integer
                    )


case class AggregatedNbaPlayerStatistics (
                                         player_name: String,
                                         statistics: Map[String, Double],
                                         current_season: Integer
                                         )