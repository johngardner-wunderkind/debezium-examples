use db;

 CREATE TABLE raw_events (
    id VARCHAR(100),
    name VARCHAR(20),
    created_at BIGINT,
    received_at BIGINT,
    md5 VARCHAR(20),
    websiteid INT,
    deviceid INT,
    source VARCHAR(5),
    version VARCHAR(5),
    og_uri VARCHAR(50),
    ip VARCHAR(20),
    user_agent VARCHAR(50)
);