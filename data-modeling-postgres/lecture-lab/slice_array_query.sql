WITH last_season AS (
SELECT * FROM zachwilson.nba_players
WHERE current_season = 2001
),
this_season AS (
SELECT * FROM bootcamp.nba_player_seasons
WHERE season = 2002
)

SELECT

  CASE WHEN
     CARDINALITY(ARRAY[ROW(season, age, weight, gp, pts, reb, ast)]
       || ls.seasons) > 3
       THEN
       SLICE(ARRAY[ROW(season, age, weight, gp, pts, reb, ast)]
       || ls.seasons, 1, 3)
     ELSE ARRAY[ROW(season, age, weight, gp, pts, reb, ast)]
       || ls.seasons
       END


FROM last_season ls FULL OUTER JOIN this_season ts
ON ls.player_name = ts.player_name