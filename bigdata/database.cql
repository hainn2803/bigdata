DROP KEYSPACE IF EXISTS trafficaccidentshub;

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS trafficaccidentshub
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

-- Use keyspace
USE trafficaccidentshub;

-- Create table for source count
CREATE TABLE IF NOT EXISTS source_count (
    window TIMESTAMP,
    severity TEXT PRIMARY KEY,
    count INT
);

-- Insert sample data
INSERT INTO source_count (window, severity, count)
VALUES (toTimestamp(now()), 'google', 1);
