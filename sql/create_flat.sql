--
--
--

-- DROP TABLE IF EXISTS public.tweet;

CREATE TABLE public.{table_name}
(
    id BIGINT PRIMARY KEY,
    text TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    geo_coordinates_0 NUMERIC,
    geo_coordinates_1 NUMERIC,
    place_full_name TEXT,
    hashtags TEXT[],
    urls TEXT[],
    truncated BOOLEAN,
    source TEXT,
    user_id BIGINT,
    user_screen_name TEXT,
    name TEXT,
    description TEXT,
    location TEXT,
    utc_offset INT,
    time_zone TEXT,
    in_reply_to_screen_name TEXT,
    in_reply_to_user_id BIGINT,
    retweet_count INTEGER,
    quote_count INTEGER,
    reply_count INTEGER,
    favorite_count INTEGER,

    user_geo_coordinates_0 NUMERIC,
    user_geo_coordinates_1 NUMERIC,
    
    user_lang TEXT,
    user_statuses_count INTEGER,
    user_followers_count INTEGER,
    user_friends_count INTEGER,
    user_created_at TIMESTAMP WITHOUT TIME ZONE,
    user_protected BOOLEAN,
    user_geo_enabled BOOLEAN,

    retweeted_status_id BIGINT,
    retweeted_status_text TEXT,
    retweeted_status_user_id BIGINT,
    retweeted_status_user_screen_name TEXT,

    quoted_status_id BIGINT,
    quoted_status_text TEXT,
    quoted_status_user_id BIGINT,
    quoted_status_user_screen_name TEXT,

    source_tweet_id BIGINT,

    tweet JSONB
);


