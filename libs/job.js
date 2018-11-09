const dblib = require('../models/local/db.js');
const Promise = require('bluebird');
const spawn = require('child_process').spawnSync;
const exec = require('child_process').exec;
const shortid = require('shortid');
const PromiseFtp = require('promise-ftp');
const md5File = require('md5-file/promise')
const path = require('path');
const mkdirp = require('mkdirp-promise');
const agaveApi = require('./agave');
const sequelize = require('../config/mysql').sequelize;
const models = require('../models/imicrobe/index');
const config = require('../config.json');
const enalib = require('../libs/ena.js');

const STATUS = {
    CREATED:         "CREATED",         // Created
    INITIALIZING:    "INITIALIZING",    // Initializing
    QUEUED:          "QUEUED",          // Waiting to be processed
    STAGING_INPUTS:  "STAGING_INPUTS",  // Transferring input files to FTP
    SUBMITTING:      "SUBMITTING",      // Submitting XML forms
    SUBMITTED:       "SUBMITTED",       // Submitted
    FINISHED:        "FINISHED",        // All steps finished successfully
    FAILED:          "FAILED",          // Non-zero return code from any step
    STOPPED:         "STOPPED"          // Cancelled due to server restart
}

const DEVELOPMENT = config.development;
const MAX_JOBS_RUNNING = 2;

class Job {
    constructor(props) {
        this.id = props.id || shortid.generate();
        this.projectId = props.projectId;
        this.username = props.username; // CyVerse username of user running the job
        this.startTime = props.startTime;
        this.endTime = props.endTime;
        this.status = props.status || STATUS.CREATED;
    }

    setStatus(newStatus) {
        if (this.status == newStatus)
            return;

        this.status = newStatus;
    }

    async init() {
        var self = this;

        const ebiConfig = config.ebiConfig;
        if (!ebiConfig)
            throw(new Error('Missing required EBI configuration'));

        this.ena = new enalib.ENA({
            id: self.id,
            username: ebiConfig.username,
            password: ebiConfig.password,
            development: DEVELOPMENT
        });

        // Save project and associated samples/files for later use
        this.project = await models.getProject(this.projectId);
    }

    async stageInputs() {
        var self = this;

        const ebi = config.ebiConfig;

        // Connect to ENA FTP
        var ftp = new PromiseFtp();
        var serverMsg = await ftp.connect({ host: ebi.hostUrl, user: ebi.username, password: ebi.password });
        console.log("ftp_connect:", serverMsg);

        // Get sequence files for all samples in project
        var files = self.project.samples
            .reduce((acc, s) => acc.concat(s.sample_files), [])
            .filter(f => {
                var file = f.file.replace(/(.gz|.gzip|.bz2|.bzip2)$/, "");
                return /(\.fasta|\.fastq|\.fa|\.fq)$/.test(file);
            });
        if (files.length == 0)
            throw(new Error('No FASTA or FASTQ inputs given'));
        console.log("Files:", files.map(f => f.file));
        self.files = files;

        // Download files via Agave and FTP to ENA
        var stagingPath = config.stagingPath + "/" + self.id;
        for (const f of self.files) {
            var filepath = f.file;//.replace("/iplant/home", "");

            // Create temp dir
            var localPath = stagingPath + path.dirname(filepath);
            await mkdirp(localPath);

            // Download file from Agave into local temp space
            var localFile = stagingPath + filepath;

            // Convert file to FASTQ if necessary
            var newFile = localFile;
            if (/(.fa|.fasta)$/.test(filepath)) {
                newFile = newFile.replace(/(.fa|.fasta)$/, "") + ".fastq.gz";
                await exec_cmd('iget -Tf ' + filepath + ' ' + localFile + ' && perl scripts/fasta_to_fastq.pl ' + localFile + ' | gzip --stdout > ' + newFile);
            }
            else if (/(.fa.gz|.fa.gzip|.fasta.gz|.fasta.gzip)$/.test(filepath)) {
                newFile = newFile.replace(/(.fa.gz|.fa.gzip|.fasta.gz|.fasta.gzip)$/, "") + ".fastq.gz";
                await exec_cmd('iget -Tf ' + filepath + ' ' + localFile + ' && gunzip --stdout ' + localFile + ' | perl scripts/fasta_to_fastq.pl | gzip --stdout > ' + newFile);
            }
            else if (/(.fa.bz2|.fa.bzip2|.fasta.bz2|.fasta.bzip2)$/.test(filepath)) {
                newFile = newFile.replace(/(.fa.bz2|.fa.bzip2|.fasta.bz2|.fasta.bzip2)$/, "") + ".fastq.gz";
                await exec_cmd('iget -Tf ' + filepath + ' ' + localFile + ' && bunzip2 --stdout ' + localFile + ' | perl scripts/fasta_to_fastq.pl | gzip --stdout > ' + newFile);
            }
            else {
                throw(new Error("Unsupported input file format: " + localFile));
            }

            // Save converted file name/path for later reference in submission
            f.dataValues.newFile = newFile;

            // Calculate MD5sum
            f.dataValues.md5sum = await md5File(newFile);
            console.log("MD5 sum:", f.dataValues.md5sum);

            // Upload file to EBI FTP
            // FIXME what if the sample files all have the same name, they will overwrite each other in FTP
            console.log("FTPing", newFile);
            await ftp.put(newFile, path.basename(newFile));
        }

        var list = await ftp.list(); // for debug
        console.log(list);

        await ftp.end();
    }

    async submit() {
        var self = this;

        var ebi = config.ebiConfig;

        const submissionXml = this.ena.generateSubmissionXml(self.project);
        const projectXml = this.ena.generateProjectXml(self.project);
        const sampleXml = this.ena.generateSampleXml(self.project.samples);

        console.log(submissionXml);
        console.log(projectXml);
        console.log(sampleXml);

        var response = await this.ena.submitProject(submissionXml, projectXml, sampleXml);

        console.log(response.RECEIPT.PROJECT);
        console.log(response.RECEIPT.SAMPLE);

        var [ experimentXml, runXml ] = this.ena.generateExperimentAndRunXml(self.files, response);

        console.log(experimentXml);
        console.log(runXml);

        var response = await this.ena.submitExperiments(submissionXml, experimentXml, runXml);

        console.log(response);
        console.log(response.RECEIPT.RUN);

        var response = await this.ena.submitRelease();
    }

    async finish() {
        var self = this;

        if (!DEVELOPMENT) {
            await Promise.all([
                self.files.map(f => {
                    var prefix = self.submissionAccession.substring(0, 6);
                    var ebiUrl = "ftp://ftp.sra.ebi.ac.uk/vol1/" + prefix + "/" + self.submissionAccession + "/fastq/" + path.basename(f.dataValues.newFile); // FIXME hardcoded base URL
                    return f.update({
                        file: ebiUrl
                    });
                })
            ]);
        }

        await models.project.update(
            {   private: 0,
                ebi_accn: self.projectAccession,
                //ebi_submission_date:
            },
            { where: { project_id: self.projectId } }
        );
    }
}

function exec_cmd(cmd_str) {
    console.log("Executing command:", cmd_str);

    return new Promise(function(resolve, reject) {
        const child = exec(cmd_str,
            (error, stdout, stderr) => {
                console.log('exec:stdout:', stdout);
                console.log('exec:stderr:', stderr);

                if (error) {
                    console.log('exec:error:', error);
                    reject(error);
                }
                else {
                    resolve(stdout);
                }
            }
        );
    });
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

        const job = await this.db.getJobBy(id);

        if (!job || (username && job.username != username))
            return;

        return self.createJob(job);
    }

    async getJobByProjectId(id, username) {
        var self = this;

        const job = await this.db.getJobByProjectId(id);

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

    async getPendingProjects() {
        return models.project.findAll({
            where: { ebi_status: "PENDING" },
            include: [ models.user ],
            logging: false
        });
    }

    createJob(job) {
        return new Job({
            id: job.job_id,
            projectId: job.project_id,
            username: job.username,
            status: job.status,
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

        return this.db.addJob(job.id, job.projectId, job.username, job.status);
    }

    async transitionJob(job, newStatus) {
        console.log('Job.transition: job ' + job.id + ' from ' + job.status + ' to ' + newStatus);
        job.setStatus(newStatus);
        await this.db.updateJob(job.id, job.status, (newStatus == STATUS.FINISHED));

        await models.project.update(
            { ebi_status: newStatus
            },
            { where: { project_id: job.projectId } }
        );
    }

    async runJob(job) {
        var self = this;

        try {
            await self.transitionJob(job, STATUS.INITIALIZING);
            await job.init();
            await self.transitionJob(job, STATUS.STAGING_INPUTS);
            await job.stageInputs();
            await self.transitionJob(job, STATUS.SUBMITTING);
            await job.submit();
            await self.transitionJob(job, STATUS.SUBMITTED);
            await job.finish();
            await self.transitionJob(job, STATUS.FINISHED);
        }
        catch(error) {
            console.log('runJob ERROR:', error);
            await self.transitionJob(job, STATUS.FAILED);
        }
    }

    async update() {
        var self = this;

        var projects = await self.getPendingProjects();
        projects.forEach(
            async project => {
                console.log("Submitting project ", project.project_id);
                var job = new Job({ projectId: project.project_id });
                job.username = project.user.user_name;
                self.submitJob(job);
            }
        );

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
