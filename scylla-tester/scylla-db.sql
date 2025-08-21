-- Create the keyspace for your project
CREATE KEYSPACE IF NOT EXISTS content_steering
WITH REPLICATION = {
  'class' : 'NetworkTopologyStrategy',
  'AWS_EU_WEST_1' : 3
}
AND TABLETS = {'enabled': false};

-- Switch to the new keyspace
USE content_steering;

-- Create the table with an optimized primary key and a new index
CREATE TABLE IF NOT EXISTS content_sessions (
                                                session_id         uuid,
                                                time               timestamp,
                                                platform           text,         -- 'mobile', 'dotcom', or 'tv'
                                                asn                int,
                                                current_cdn        text,         -- The partition key
                                                video_profile_kbps int,
                                                PRIMARY KEY ((current_cdn), time, session_id)
    ) WITH CLUSTERING ORDER BY (time DESC);

CREATE TABLE IF NOT EXISTS sessions_by_asn (
                                               session_id         uuid,
                                               time               timestamp,
                                               platform           text,
                                               asn                int,          -- The partition key
                                               current_cdn        text,
                                               video_profile_kbps int,
                                               PRIMARY KEY ((asn), time, session_id)
    ) WITH CLUSTERING ORDER BY (time DESC);