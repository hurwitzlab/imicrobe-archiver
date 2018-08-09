const dblib = require('../models/local/db.js');
const Promise = require('bluebird');
const sequence = require('promise-sequence');
const spawn = require('child_process').spawnSync;
const exec = require('child_process').exec;
const pathlib = require('path');
const shortid = require('shortid');
const requestp = require('request-promise');
const PromiseFtp = require('promise-ftp');
const path = require('path');
const mkdirp = require('mkdirp-promise');
const xml2js = require('xml2js');
const fs = require('fs');
const agaveApi = require('./agave');
const sequelize = require('../config/mysql').sequelize;
const models = require('../models/imicrobe/index');
const config = require('../config.json');

const STATUS = {
    CREATED:         "CREATED",         // Created
    QUEUED:          "QUEUED",          // Waiting to be processed
    STAGING_INPUTS:  "STAGING_INPUTS",  // Transferring input files to FTP
    SUBMITTING:      "SUBMITTING",      // Submitting XML forms
    FINISHED:        "FINISHED",        // All steps finished successfully
    FAILED:          "FAILED",          // Non-zero return code from any step
    STOPPED:         "STOPPED"          // Cancelled due to server restart
}

const MAX_JOBS_RUNNING = 2;

class Job {
    constructor(props) {
        this.id = props.id || shortid.generate();
        this.projectId = props.projectId;
        this.username = props.username; // CyVerse username of user running the job
        this.token = props.token;
        this.startTime = props.startTime;
        this.endTime = props.endTime;
        this.status = props.status || STATUS.CREATED;
    }

    setStatus(newStatus) {
        if (this.status == newStatus)
            return;

        this.status = newStatus;
    }

    stageInputs() {
        var self = this;

        const ebi = config.ebiConfig;
        if (!ebi)
            throw(new Error('Missing required EBI configuration'));

        var stagingPath = config.stagingPath + "/" + self.id;

        var ftp = new PromiseFtp();

        return ftp.connect({ host: ebi.hostUrl, user: ebi.username, password: ebi.password })
            .then( serverMsg => {
                console.log("ftp_connect:", serverMsg);
                return models.getProject(self.projectId);
            })
            .then( project => {
                var files = project.samples
                    .reduce((acc, s) => acc.concat(s.sample_files), [])
                    .filter(f => {
                        var file = f.file.replace(/(.gz|.gzip|.bz2|.bzip2)$/, "");
                        return /(\.fasta|\.fastq|\.fa|\.fq)$/.test(file);
                    });
                if (files.length == 0)
                    throw(new Error('No FASTA or FASTQ inputs given'));
                console.log("Files:", files.map(f => f.file));

                var p = [];
                files.forEach(f => {
                    var filepath = f.file.replace("/iplant/home", "");

                    // Create temp dir
                    var localPath = stagingPath + path.dirname(filepath);
                    p.push( () => mkdirp(localPath) );

                    // Download file from Agave into local temp space
                    var localFile = stagingPath + filepath;
                    var agave = new agaveApi.AgaveAPI({ token: self.token });
                    p.push( () => agave.filesGet(filepath, localFile) );
                    //FIXME Agave error json response needs to be detected

                    // Convert file to FASTQ if necessary
                    var newFile = localFile;
                    if (/(.fa|.fasta)$/.test(filepath)) {
                        newFile = newFile.replace(/(.fa|.fasta)$/, "") + ".fastq.gz";
                        p.push( () => exec_cmd('perl scripts/fasta_to_fastq.pl ' + localFile + ' | gzip --stdout > ' + newFile) );
                    }
                    else if (/(.fa.gz|.fa.gzip|.fasta.gz|.fasta.gzip)$/.test(filepath)) {
                        newFile = newFile.replace(/(.fa.gz|.fa.gzip|.fasta.gz|.fasta.gzip)$/, "") + ".fastq.gz";
                        p.push( () => exec_cmd('gunzip --stdout ' + localFile + ' | perl scripts/fasta_to_fastq.pl | gzip --stdout > ' + newFile) );
                    }
                    else if (/(.fa.bz2|.fa.bzip2|.fasta.bz2|.fasta.bzip2)$/.test(filepath)) {
                        newFile = newFile.replace(/(.fa.bz2|.fa.bzip2|.fasta.bz2|.fasta.bzip2)$/, "") + ".fastq.gz";
                        p.push( () => exec_cmd('bunzip2 --stdout ' + localFile + ' | perl scripts/fasta_to_fastq.pl | gzip --stdout > ' + newFile) );
                    }

                    // Upload file to EBI FTP
                    // FIXME what if the sample files all have the same name, they will overwrite each other in FTP
                    p.push( () => {
                        console.log("FTPing", newFile);
                        return ftp.put(newFile, path.basename(newFile))
                    });
                });

                return sequence.pipeline(p);
            })
            .then(function () { // for debug
                return ftp.list();
            })
            .then(function (list) {
                console.log(list);
                return ftp.end();
            })
            .catch(console.error);
    }

    submit() {
        var self = this;

        var ebi = config.ebiConfig;

        var builder = new xml2js.Builder();

        var submissionXml = builder.buildObject({
            SUBMISSION: {
                $: { center_name: "Hurwitz Lab" },
                ACTIONS: {
                    ACTION: {
                        ADD: {}
                    }
                }
            }
        });

        var projectXml = builder.buildObject({
            PROJECT_SET: {
                PROJECT: {
                    $: { alias: "imicrobe_programmatic_study", center_name: "Hurwitz Lab" },
                    TITLE: "Demonstration of Programmatic Data Submission",
                    DESCRIPTION: "A demonstration of programmatic data submission.",
                    SUBMISSION_PROJECT: {
                        SEQUENCING_PROJECT: {}
                    }
                }
            }
        });

        var sampleXml = builder.buildObject({
          "SAMPLE_SET": {
            "SAMPLE": {
              $: { "alias": "IMICROBESAMPLE", "center_name": "Hurwitz Lab" },
              "TITLE": "human gastric microbiota, mucosal",
              "SAMPLE_NAME": {
                "TAXON_ID": "1284369",
                "SCIENTIFIC_NAME": "stomach metagenome"
              },
              "SAMPLE_ATTRIBUTES": {
                "SAMPLE_ATTRIBUTE": [
                  {
                    "TAG": "project name",
                    "VALUE": "imicrobe_programmatic_study"
                  },
                  {
                    "TAG": "investigation type",
                    "VALUE": "mimarks-survey"
                  },
                  {
                    "TAG": "sequencing method",
                    "VALUE": "pyrosequencing"
                  },
                  {
                    "TAG": "collection date",
                    "VALUE": "2010"
                  },
                  {
                    "TAG": "host body site",
                    "VALUE": "Mucosa of stomach"
                  },
                  {
                    "TAG": "human-associated environmental package",
                    "VALUE": "human-associated"
                  },
                  {
                    "TAG": "geographic location (latitude)",
                    "VALUE": "1.81",
                    "UNITS": "DD"
                  },
                  {
                    "TAG": "geographic location (longitude)",
                    "VALUE": "-78.76",
                    "UNITS": "DD"
                  },
                  {
                    "TAG": "geographic location (country and/or sea)",
                    "VALUE": "Colombia"
                  },
                  {
                    "TAG": "geographic location (region and locality)",
                    "VALUE": "Tumaco"
                  },
                  {
                    "TAG": "environment (biome)",
                    "VALUE": "coast"
                  },
                  {
                    "TAG": "environment (feature)",
                    "VALUE": "human-associated habitat"
                  },
                  {
                    "TAG": "environment (material)",
                    "VALUE": "gastric biopsy"
                  },
                  {
                    "TAG": "ENA-CHECKLIST",
                    "VALUE": "ERC000014"
                  }
                ]
              }
            }
          }
        });

        var experimentXml = builder.buildObject({
          "EXPERIMENT_SET": {
            "EXPERIMENT": {
              $: { "alias": "imicrobe_exp", "center_name": "Hurwitz Lab" },
              "TITLE": "The 1KITE project: evolution of insects",
              "STUDY_REF": { $: { "accession": "SRP017801" } },
              "DESIGN": {
                "DESIGN_DESCRIPTION": {},
                "SAMPLE_DESCRIPTOR": { $: { "accession": "SRS462875" } },
                "LIBRARY_DESCRIPTOR": {
                  "LIBRARY_STRATEGY": "RNA-Seq",
                  "LIBRARY_SOURCE": "TRANSCRIPTOMIC",
                  "LIBRARY_SELECTION": "cDNA",
                  "LIBRARY_LAYOUT": {
                    "SINGLE": {}
                  },
                  "LIBRARY_CONSTRUCTION_PROTOCOL": "Messenger RNA (mRNA) was isolated using the Dynabeads mRNA Purification Kit (Invitrogen, Carlsbad Ca. USA) and then sheared using divalent cations at 72*C. These cleaved RNA fragments were transcribed into first-strand cDNA using II Reverse Transcriptase (Invitrogen, Carlsbad Ca. USA) and N6 primer (IDT). The second-strand cDNA was subsequently synthesized using RNase H (Invitrogen, Carlsbad Ca. USA) and DNA polymerase I (Invitrogen, Shanghai China). The double-stranded cDNA then underwent end-repair, a single `A? base addition, adapter ligati on, and size selection on anagarose gel (250 * 20 bp). At last, the product was indexed and PCR amplified to finalize the library prepration for the paired-end cDNA."
                }
              },
              "PLATFORM": {
                "ILLUMINA": { "INSTRUMENT_MODEL": "Illumina HiSeq 2000" }
              },
              "EXPERIMENT_ATTRIBUTES": {
                "EXPERIMENT_ATTRIBUTE": {
                  "TAG": "library preparation date",
                  "VALUE": "2010-08"
                }
              }
            }
          }
        });

        var runXml = builder.buildObject({
          "RUN_SET": {
            "RUN": {
              $: { "alias": "imicrobe_run", "center_name": "Hurwitz Lab" },
              "EXPERIMENT_REF": { $: { "refname": "imicrobe_exp" } },
              "DATA_BLOCK": {
                "FILES": {
                  "FILE": {
                    $: {
                      "filename": "POV_GD.Spr.C.8m_reads.fa",
                      "filetype": "fastq",
                      "checksum_method": "MD5",
                      "checksum": "ccae9861270be267f04b45a4d90718be"
                    }
                  }
                }
              }
            }
          }
        });

        var tmpPath = "./tmp/"; //config.stagingPath + "/" + self.id + "/";

        return Promise.all([ // Is there a way to stream these XML docs from memory instead of writing to file first?
                writeFile(tmpPath + '__submission__.xml', submissionXml),
                writeFile(tmpPath + '__project__.xml', projectXml),
                writeFile(tmpPath + '__sample__.xml', sampleXml),
                writeFile(tmpPath + '__experiment__.xml', experimentXml),
                writeFile(tmpPath + '__run__.xml', runXml)
            ])
            .then(() => {
                var options = {
                    method: "POST",
                    uri: ebi.submissionUrl,
                    headers: {
                        "Authorization": "Basic " + new Buffer(ebi.username + ":" + ebi.password).toString('base64'),
                        "Accept": "application/xml",
                    },
                    formData: {
                        SUBMISSION: {
                            value: fs.createReadStream(tmpPath + '__submission__.xml'),
                            options: {
                                filename: 'SUBMISSION.xml',
                                contentType: 'application/xml'
                            }
                        },
                        PROJECT: {
                            value: fs.createReadStream(tmpPath + '__project__.xml'),
                            options: {
                                filename: 'PROJECT.xml',
                                contentType: 'application/xml'
                            }
                        },
                        SAMPLE: {
                            value: fs.createReadStream(tmpPath + '__sample__.xml'),
                            options: {
                                filename: 'SAMPLE.xml',
                                contentType: 'application/xml'
                            }
                        },
                        EXPERIMENT: {
                            value: fs.createReadStream(tmpPath + '__experiment__.xml'),
                            options: {
                                filename: 'EXPERIMENT.xml',
                                contentType: 'application/xml'
                            }
                        },
                        RUN: {
                            value: fs.createReadStream(tmpPath + '__run__.xml'),
                            options: {
                                filename: 'RUN.xml',
                                contentType: 'application/xml'
                            }
                        }
                    }
                };

                return requestp(options)
                    .then(function (parsedBody) {
                        console.log(parsedBody);
                    })
            })
            .catch(console.error);
    }
}

function writeFile(filepath, data) {
    return new Promise(function(resolve, reject) {
        fs.writeFile(filepath, data, 'UTF-8', function(err) {
            if (err) reject(err);
            else resolve(data);
        });
    });
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
            projectId: job.project_id,
            username: job.username,
            token: job.token,
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

        return this.db.addJob(job.id, job.projectId, job.username, job.token, job.status);
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
//        .then( () => self.transitionJob(job, STATUS.SUBMITTING) )
//        .then( () => { return job.submit() })
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
