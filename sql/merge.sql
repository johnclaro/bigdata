SELECT
    event.game_id,
    event,
    white,
    black,
    utc_date,
    utc_time,
    white_elo,
    black_elo,
    white_rating_diff,
    black_rating_diff,
    eco,
    opening,
    time_control,
    termination
FROM event
INNER JOIN white ON event.game_id = white.game_id
INNER JOIN black ON event.game_id = black.game_id
INNER JOIN result ON event.game_id = result.game_id
INNER JOIN utc_date ON event.game_id = utc_date.game_id
INNER JOIN utc_time ON event.game_id = utc_time.game_id
INNER JOIN white_elo ON event.game_id = white_elo.game_id
INNER JOIN black_elo ON event.game_id = black_elo.game_id
INNER JOIN white_rating_diff ON event.game_id = white_rating_diff.game_id
INNER JOIN black_rating_diff ON event.game_id = black_rating_diff.game_id
INNER JOIN eco ON event.game_id = eco.game_id
INNER JOIN opening ON event.game_id = opening.game_id
INNER JOIN time_control ON event.game_id = time_control.game_id
INNER JOIN termination ON event.game_id = termination.game_id