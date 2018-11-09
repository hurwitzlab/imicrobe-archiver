const sqlite = require('sqlite');
const Promise = require('bluebird');
const dateFormat = require('dateformat');

class Database {
    constructor() {}

    open(sqliteFilePath) {
        if (!sqliteFilePath) {
            console.error('Missing sqliteFilePath argument');
            return;
        }

        return sqlite.open(sqliteFilePath, { Promise });
    }

    getJob(jobId) {
        return sqlite.get("SELECT job_id, project_id, username, status, start_time, end_time FROM jobs WHERE job_id=?", jobId);
    }

    getJobByProjectId(projectId) {
        return sqlite.get("SELECT job_id, project_id, username, status, start_time, end_time FROM jobs WHERE project_id=?", projectId);
    }

    getJobs() {
        return sqlite.all("SELECT job_id, project_id, username, status, start_time, end_time FROM jobs");
    }

    getJobsForUser(username) {
        return sqlite.all("SELECT job_id, project_id, username, status, start_time, end_time FROM jobs WHERE username=?", username);
    }

    getActiveJobs() {
        return sqlite.all("SELECT job_id, project_id, username, status, start_time, end_time FROM jobs WHERE status NOT IN ('STOPPED', 'FINISHED', 'FAILED')");
    }

    addJob(job_id, project_id, username, status) {
        let start_time = getTimestamp();
        return sqlite.run("INSERT INTO jobs (job_id, project_id, username, status, start_time) VALUES (?,?,?,?,?)", [job_id, project_id, username, status, start_time]);
    }

    updateJob(job_id, status, isEnded) {
        let end_time = ( isEnded ? getTimestamp() : null );
        return sqlite.run("UPDATE jobs SET status=?, end_time=? WHERE job_id=?", [status, end_time, job_id]);
    }

    stopJobs() {
        return sqlite.run("UPDATE jobs SET status='STOPPED' WHERE status NOT IN ('STOPPED', 'FINISHED', 'FAILED')");
    }
}

function getTimestamp() {
    let now = new Date();
    return dateFormat(now, "yyyy-mm-dd") + "T" + dateFormat(now, "HH:MM:ss.lo"); // dateFormat(now, "isoDateTime");
}

exports.Database = Database;
exports.getTimestamp = getTimestamp;