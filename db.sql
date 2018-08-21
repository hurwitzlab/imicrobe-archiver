CREATE TABLE jobs (
    job_id     TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL,
    username   TEXT NOT NULL,
    status     TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time   TEXT
);
