SELECT player_name, seasons[1].pts, seasons[1].pts / CASE WHEN CARDINALITY(seasons) < 2 THEN NULL ELSE seasons[2].pts END as pct_change FROM zachwilson.nba_players
WHERE current_season = 2001
AND is_active