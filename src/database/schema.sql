-- schema.sql

CREATE TABLE IF NOT EXISTS player_stats (
    -- Metadata --
    id UUID PRIMARY KEY,
    ballchasing_id VARCHAR(50) UNIQUE,
    player_name VARCHAR(100),
    player_id VARCHAR(100),

    -- Core --
    core_shots INT,
    core_goals INT,
    core_saves INT,
    core_assists INT,
    core_score INT,
    core_mvp BOOLEAN,
    core_shooting_pct FLOAT,

    -- Boost --
    boost_bpm FLOAT,
    boost_bcpm FLOAT,
    boost_avg_amount FLOAT,
    boost_amount_collected FLOAT,
    boost_amount_stolen FLOAT,
    boost_amount_overfill FLOAT,
    boost_amount_overfill_stolen FLOAT,
    boost_amount_used_supersonic FLOAT,
    boost_time_zero_boost FLOAT,
    boost_time_full_boost FLOAT,
    boost_time_boost_0_25 FLOAT,
    boost_time_boost_25_50 FLOAT,
    boost_time_boost_50_75 FLOAT,
    boost_time_boost_75_100 FLOAT,
    boost_big_pads_collected INT,
    boost_small_pads_collected INT,
    boost_big_pads_stolen INT,
    boost_small_pads_stolen INT,

    -- Positioning --
    pos_avg_dist_ball FLOAT,
    pos_avg_dist_ball_possession FLOAT,
    pos_avg_dist_ball_no_possession FLOAT,
    pos_avg_dist_teammates FLOAT,
    pos_time_behind_ball FLOAT,
    pos_time_front_ball FLOAT,
    pos_time_most_back FLOAT,
    pos_time_most_forward FLOAT,
    pos_time_closest_to_ball FLOAT,
    pos_time_farthest_from_ball FLOAT,
    pos_percent_defensive_third FLOAT,
    pos_percent_neutral_third FLOAT,
    pos_percent_offensive_third FLOAT,
    pos_percent_defensive_half FLOAT,
    pos_percent_offensive_half FLOAT,

    -- Movement --
    mov_avg_speed FLOAT,
    mov_total_distance FLOAT,
    mov_time_supersonic_speed FLOAT,
    mov_time_boost_speed FLOAT,
    mov_time_slow_speed FLOAT,
    mov_percent_supersonic_speed FLOAT,
    mov_percent_boost_speed FLOAT,
    mov_percent_slow_speed FLOAT,
    mov_time_ground FLOAT,
    mov_time_low_air FLOAT,
    mov_time_high_air FLOAT,
    mov_percent_ground FLOAT,
    mov_percent_low_air FLOAT,
    mov_percent_high_air FLOAT,
    mov_time_powerslide FLOAT,
    mov_count_powerslide INT,
    mov_avg_powerslide_duration FLOAT,

    -- Playstyle --
    demo_inflicted INT,
    demo_taken INT,

    -- Contextual --
    match_win BOOLEAN,
    match_duration INT,
    team_color VARCHAR(10),
    playlist_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
