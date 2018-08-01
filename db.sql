CREATE TABLE jobs (
    submission_id TEXT PRIMARY KEY,
    username   TEXT NOT NULL,
    token      TEXT,
    status     TEXT NOT NULL,
    inputs     TEXT,
    start_time TEXT NOT NULL,
    end_time   TEXT
);
