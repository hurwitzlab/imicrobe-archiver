const dblib = require('../db.js');
const Promise = require('bluebird');
const spawn = require('child_process').spawnSync;
const execFile = require('child_process').execFile;
const pathlib = require('path');
const shortid = require('shortid');
const requestp = require('request-promise');
const PromiseFtp = require('promise-ftp');
const path = require('path');
const mkdirp = require('mkdirp-promise');
const agaveApi = require('../agave');
const config = require('../../config.json');
const fs = require('fs');

const STATUS = {
    CREATED:         "CREATED",         // Created/queued
    STAGING_INPUTS:  "STAGING_INPUTS",  // Transferring input files to FTP
    FINISHED:        "FINISHED",        // All steps finished successfully
    FAILED:          "FAILED",          // Non-zero return code from any step
    STOPPED:         "STOPPED"          // Cancelled due to server restart
}

const MAX_JOBS_RUNNING = 2;

class Job {
    constructor(props) {
        this.id = props.id || shortid.generate();
        this.username = props.username; // CyVerse username of user running the job
        this.token = props.token;
        this.startTime = props.startTime;
        this.endTime = props.endTime;
        this.inputs = props.inputs || {};
        this.status = props.status || STATUS.CREATED;
    }

    setStatus(newStatus) {
        if (this.status == newStatus)
            return;

        this.status = newStatus;
    }

    stageInputs() {
        var self = this;

        var stagingPath = config.stagingPath + "/" + self.id;

        const ebi = config.ebiConfig;
        if (!ebi)
            throw(new Error('Missing required EBI configuration'));

        var ftp = new PromiseFtp();

        return ftp.connect({ host: ebi.hostUrl, user: ebi.username, password: ebi.password })
            .then( serverMsg => {
                console.log("ftp_connect:", serverMsg);
                return Promise.each(this.inputs, filepath => {
                    // Download file from Agave into local temp space
                    var localPath = stagingPath + path.dirname(filepath);
                    return mkdirp(localPath)
                    .then( () => {
                        var agave = new agaveApi.AgaveAPI({ token: self.token });
                        return agave.filesGet(filepath, stagingPath + filepath);
                    })
                    .then( () => {
                        // Upload file to EBI FTP
                        // FIXME what if the sample files all have the same name, they will overwrite each other in FTP
                        console.log("FTPing file " + filepath + " to " + ebi.hostUrl);
                        return ftp.put(stagingPath + filepath, path.basename(filepath));
                    });
                })
            })
            .then(function () {
                return ftp.list();
            })
            .then(function (list) {
                console.log(list);
                return ftp.end();
            })
            .catch(console.error);
    }

}

class JobManager {
    constructor(props) {
        this.isMaster = props.isMaster;
        this.UPDATE_INITIAL_DELAY = 5000; // milliseconds
        this.UPDATE_REFRESH_DELAY = 5000; // milliseconds

        this.init();
    }

    async init() {
        var self = this;

        console.log("JobManager.init");

        this.db = new dblib.Database();
        await this.db.open(config.dbFilePath);

        // Set pending jobs to cancelled
        if (this.isMaster) {
            console.log("Setting all jobs to STOPPED");
            await this.db.stopJobs();
        }

        // Start update loop
        if (this.isMaster) {
            console.log("Starting main update loop");
            setTimeout(() => {
                self.update();
            }, this.UPDATE_INITIAL_DELAY);
        }
    }

    async getJob(id, username) {
        var self = this;

        const job = await this.db.getJob(id);

        if (!job || (username && job.username != username))
            return;

        return self.createJob(job);
    }

    async getJobs(username) {
        var self = this;
        var jobs;

        if (username)
            jobs = await this.db.getJobsForUser(username);
        else
            jobs = await this.db.getJobs();

        return jobs.map( job => { return self.createJob(job) } );
    }

    async getActiveJobs() {
        var self = this;

        const jobs = await this.db.getActiveJobs();

        return jobs.map( job => { return self.createJob(job) } );
    }

    createJob(job) {
        return new Job({
            id: job.job_id,
            username: job.username,
            token: job.token,
            status: job.status,
            inputs: JSON.parse(job.inputs),
            startTime: job.start_time,
            endTime: job.end_time
        });
    }

    submitJob(job) {
        console.log("JobManager.submitJob", job.id);

        if (!job) {
            console.error("JobManager.submitJob: missing job");
            return;
        }

        return this.db.addJob(job.id, job.username, job.token, job.status, JSON.stringify(job.inputs));
    }

    async transitionJob(job, newStatus) {
        console.log('Job.transition: job ' + job.id + ' from ' + job.status + ' to ' + newStatus);
        job.setStatus(newStatus);
        await this.db.updateJob(job.id, job.status, (newStatus == STATUS.FINISHED));
    }

    runJob(job) {
        var self = this;

        self.transitionJob(job, STATUS.STAGING_INPUTS)
        .then( () => { return job.stageInputs() })
//        .then( () => self.transitionJob(job, STATUS.RUNNING) )
//        .then( () => { return job.runLibra() })
//        .then( () => self.transitionJob(job, STATUS.ARCHIVING) )
//        .then( () => { return job.archive() })
        .then( () => self.transitionJob(job, STATUS.FINISHED) )
        .catch( error => {
            console.log('runJob ERROR:', error);
            self.transitionJob(job, STATUS.FAILED);
        });
    }

    async update() {
        var self = this;

        //console.log("Update ...")
        var jobs = await self.getActiveJobs();
        if (jobs && jobs.length) {
            var numJobsRunning = jobs.reduce( (sum, value) => {
                if (value.status == STATUS.RUNNING)
                    return sum + 1
                else return sum;
            } );

            await jobs.forEach(
                async job => {
                    //console.log("update: job " + job.id + " is " + job.status);
                    if (numJobsRunning >= MAX_JOBS_RUNNING)
                        return;

                    if (job.status == STATUS.CREATED) {
                        console.log
                        self.runJob(job);
                        numJobsRunning++;
                    }
                }
            );
        }

        setTimeout(() => {
            self.update();
        }, this.UPDATE_REFRESH_DELAY);
    }
}

exports.Job = Job;
exports.JobManager = JobManager;
