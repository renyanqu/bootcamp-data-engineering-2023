from chispa.dataframe_comparer import *

from ..jobs.udf_example import add_udf_columns_transformation
from collections import namedtuple

NbaPlayer = namedtuple("NbaPlayer", "player_name seasons current_season")
NbaDeltaOutput = namedtuple("NbaDeltaOutput", "player_name max_consecutive_20pt_seasons max_consecutive_10pt_seasons biggest_season_point_delta average_point_delta current_season")
Season = namedtuple("Season", "pts rebs season")

input_data = [
    NbaPlayer(
        player_name='Michael Jordan',
        seasons=[
            Season(
                pts=20.1,
                rebs=7.0,
                season=2002
            ),
            Season(
                pts=20.1,
                rebs=7.0,
                season=2003
            ),
            Season(
                pts=18.0,
                rebs=7.0,
                season=2004
            )
        ],
        current_season=2005
    ),
    NbaPlayer(
        player_name='Null Wilson',
        seasons=[],
        current_season=2005
    )
]


def test_udf_example(spark):
    fake_input_data = spark.createDataFrame(input_data)
    actual_output_data = add_udf_columns_transformation(fake_input_data)
    expected_output_data = [
        NbaDeltaOutput(
            player_name='Michael Jordan',
            current_season=2005,
            max_consecutive_20pt_seasons=2,
            max_consecutive_10pt_seasons=3,
            biggest_season_point_delta=2.1,
            average_point_delta=0.7
        ),
        NbaDeltaOutput(
            player_name='Null Wilson',
            current_season=2005,
            max_consecutive_20pt_seasons=0,
            max_consecutive_10pt_seasons=0,
            biggest_season_point_delta=None,
            average_point_delta=None
        )
    ]
    expected_output_data_df = spark.createDataFrame(expected_output_data)
    assert_df_equality(actual_output_data, expected_output_data_df, ignore_nullable=True)