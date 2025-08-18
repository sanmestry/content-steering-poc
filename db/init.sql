-- Create the content_sessions hypertable
CREATE TABLE IF NOT EXISTS content_sessions (
                                                time TIMESTAMPTZ NOT NULL,
                                                session_id UUID,
                                                platform TEXT,
                                                channel TEXT,
                                                asn INT,
                                                current_cdn TEXT,
                                                video_profile_kbps INT
);
SELECT create_hypertable('content_sessions', 'time', if_not_exists => TRUE);

-- Create the cdn_capacity table
CREATE TABLE IF NOT EXISTS cdn_capacity (
                                            cdn_name TEXT PRIMARY KEY,
                                            max_capacity_mbps INT
);

-- Insert sample data into cdn_capacity
INSERT INTO cdn_capacity (cdn_name, max_capacity_mbps) VALUES
                                                           ('Akamai', 500),
                                                           ('Fastly', 750),
                                                           ('CloudFront', 300)
    ON CONFLICT (cdn_name) DO NOTHING;


-- Create the cdn_priority table
CREATE TABLE IF NOT EXISTS cdn_priority (
                                            asn INT,
                                            priority_level INT,
                                            cdn_name TEXT,
                                            PRIMARY KEY (asn, priority_level)
    );

-- Insert sample data into cdn_priority
INSERT INTO cdn_priority (asn, priority_level, cdn_name) VALUES
                                                             (0, 1, 'Akamai'),
                                                             (0, 2, 'Fastly'),
                                                             (0, 3, 'CloudFront'),
                                                             (2856, 1, 'CloudFront'),
                                                             (2856, 2, 'Akamai'),
                                                             (2856, 3, 'Fastly')
    ON CONFLICT (asn, priority_level) DO NOTHING;