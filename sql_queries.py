import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                        se_id               int         IDENTITY(0,1),
                                        artist              varchar,
                                        auth                varchar,
                                        firstName           varchar,
                                        gender              varchar,
                                        itemInSession       int,
                                        lastName            varchar,
                                        length              float,
                                        level               varchar,
                                        location            varchar,
                                        method              varchar,
                                        page                varchar,
                                        registration        float,
                                        session_id           bigint,
                                        song                varchar,
                                        status              int,
                                        ts                  bigint,
                                        userAgent           varchar,
                                        user_id             bigint
                                        )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                        ss_id               int         IDENTITY(0,1),
                                        num_songs           int,
                                        artist_id           varchar,
                                        artist_latitude     float,
                                        artist_longitude    float,
                                        artist_location     varchar,
                                        artist_name         varchar,
                                        song_id             varchar,
                                        title               varchar,
                                        duration            float,
                                        year                int
                                        )
                                        
""")

# facttable
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                songplay_id         bigint          IDENTITY(0,1)   UNIQUE,
                                start_time          timestamp       REFERENCES time     sortkey ,
                                user_id             int             NOT NULL    distkey,
                                level               varchar, 
                                song_id             varchar,
                                artist_id           varchar,
                                session_id          int, 
                                location            varchar, 
                                user_agent          varchar
                                )
""")

# dimension tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id         int         PRIMARY KEY     NOT NULL    distkey sortkey,
                                first_name      varchar,
                                last_name       varchar,
                                gender          varchar,
                                level           varchar)
                                diststyle key;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                                song_id         varchar     PRIMARY KEY     NOT NULL    distkey sortkey,
                                title           varchar     NOT NULL, 
                                artist_id       varchar     NOT NULL,
                                year            int,
                                duration        float)
                                diststyle key;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id       varchar     PRIMARY KEY distkey sortkey,
                                name            varchar     NOT NULL,
                                location        varchar,
                                latitude        float,
                                longitude       float)
                                diststyle key;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time      timestamp     PRIMARY KEY     NOT NULL    sortkey,
                                hour            int         NOT NULL    distkey,
                                day             int         NOT NULL,
                                week            int         NOT NULL,
                                month           int         NOT NULL,
                                year            int         NOT NULL,
                                weekday         varchar     NOT NULL)
                                diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}  
credentials 'aws_iam_role={}'
json {}
compupdate off
region 'us-west-2';
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}'
TRUNCATECOLUMNS
ACCEPTINVCHARS
compupdate off
region 'us-west-2'
JSON 'auto';
""").format(config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,
                                                  user_agent)
                            SELECT DISTINCT
                                    DATE_ADD('ms', sd.ts, '1970-01-01') AS start_time,
                                    sd.user_id as user_id,
                                    sd.level as level,
                                    ss.song_id as song_id,
                                    ss.artist_id as artist_id,
                                    sd.session_id as session_id,
                                    sd.location as location,
                                    sd.userAgent as user_agent
                            FROM staging_events sd
                            LEFT JOIN staging_songs ss
                            on (sd.song = ss.title)
                            WHERE sd.page = 'NextSong'
                            AND sd.user_id IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)
                        SELECT 
                            sd.user_id as user_id,
                            sd.firstName as first_name,
                            sd.lastName as last_name,
                            sd.gender as gender,
                            sd.level as level
                        FROM staging_events sd
                        WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration)
                        SELECT
                            song_id as song_id,
                            title as title,
                            artist_id as artist_id,
                            year as year,
                            duration as duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,latitude,longitude)
                          SELECT
                            artist_id as artist_id,
                            artist_name as artist,
                            artist_location as location,
                            artist_latitude as latitude,
                            artist_longitude as longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
                        EXTRACT(HOUR from start_time),
                        EXTRACT(DAY from start_time),
                        EXTRACT(WEEK from start_time),
                        EXTRACT(MONTH from start_time),
                        EXTRACT(YEAR from start_time),
                        EXTRACT(DOW from start_time)
                        FROM staging_events
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]