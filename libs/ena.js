const https = require('https');
const path = require('path');
const xml2js = require('xml2js');
const requestp = require('request-promise');
const config = require('../config.json');

const DEV_SUBMISSION_URL = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/";
const PROD_SUBMISSION_URL = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/";

class ENA {
    constructor(props) {
        this.id = props.id;
        this.username = props.username;
        this.password = props.password;
        this.development = props.development;

        if (this.development)
            this.submissionUrl = DEV_SUBMISSION_URL;
        else
            this.submissionUrl = PROD_SUBMISSION_URL;

        this.builder = new xml2js.Builder();
    }

    generateSubmissionXml(project) {
        return this.builder.buildObject({
            SUBMISSION: {
                $: { center_name: project.institution },
                ACTIONS: {
                    ACTION: {
                        ADD: {}
                    },
//                    ACTION: {
//                        VALIDATE: {}
//                    },
                }
            }
        });
    }

    generateProjectXml(project) {
        if (!project.institution)
            throw(new Error("Missing project institution field"));

        let projectLinks = [];
        project.publications.forEach(pub => {
            if (pub.pubmed_id) {
                projectLinks.push({
                    PROJECT_LINK: {
                        XREF_LINK: {
                            DB: "PUBMED",
                            ID: pub.pubmed_id
                        }
                    }
                });
            }
        });

        let projectAlias = "project_" + (project.project_code ? project.project_code : project.project_id) + "_" + this.id;
        let projectXml = this.builder.buildObject({
            PROJECT_SET: {
                PROJECT: {
                    $: { alias: projectAlias },
                    TITLE: project.project_name,
                    DESCRIPTION: project.description,
                    SUBMISSION_PROJECT: {
                        SEQUENCING_PROJECT: {}
                    },
                    PROJECT_LINKS: projectLinks
                }
            }
        });

        return projectXml;
    }

    generateSampleXml(samples) {
        let self = this;

        let sampleSetObj = { SAMPLE_SET: [] };

        self.samplesByAlias = {};
        //self.filesByAlias = {};

        samples.forEach(sample => {
            let sampleAlias = "sample_"  + (sample.sample_acc ? sample.sample_acc : sample.sample_id) + "_" + this.id;
            self.samplesByAlias[sampleAlias] = sample;

            // FIXME this code block repeated below
            let attrs = {};
            sample.sample_attrs.forEach(attr => {
                let key = attr.sample_attr_type.type.toLowerCase();
                attrs[key] = attr.attr_value;
            });

            if (!attrs["taxon_id"])
                throw(new Error("Missing taxon_id attribute for Sample '" + sample.sample_name + "'"));

            let sampleObj = {
                SAMPLE: {
                  $: { alias: sampleAlias },
                  TITLE: sample.sample_name,
                  SAMPLE_NAME: {
                    TAXON_ID: attrs["taxon_id"],
                  }
                }
            };

            let attrObj = [];
            sample.sample_attrs.forEach(attr => {
                attrObj.push({
                    SAMPLE_ATTRIBUTE: {
                        TAG: attr.sample_attr_type.type,
                        VALUE: attr.attr_value
                    }
                });
            });

            // Weird but needed to get proper format
            sampleObj.SAMPLE.SAMPLE_ATTRIBUTES = [];
            sampleObj.SAMPLE.SAMPLE_ATTRIBUTES.push(attrObj);

            sampleSetObj.SAMPLE_SET.push(sampleObj);
        });

        return this.builder.buildObject(sampleSetObj);
    }

    generateExperimentAndRunXml(files, response) {
        let self = this;

        let experimentSetObj = { EXPERIMENT_SET: [] };
        let runSetObj = { RUN_SET: [] };

        response.RECEIPT.SAMPLE.forEach(sampleRes => {
            let sampleAccession = sampleRes.$.accession;
            let sampleAlias = sampleRes.$.alias;
            let sample = self.samplesByAlias[sampleAlias];

            self.projectAccession = response.RECEIPT.PROJECT[0].$.accession;
            self.submissionAccession = response.RECEIPT.SUBMISSION[0].$.accession;

            // FIXME this code block repeated above
            let attrs = {};
            sample.sample_attrs.forEach(attr => {
                let key = attr.sample_attr_type.type.toLowerCase();
                attrs[key] = attr.attr_value;
            });
            console.log(attrs);

            if (!attrs["library_strategy"])
                throw(new Error("Missing library_strategy attribute for Sample '" + sample.sample_name + "'"));
            if (!attrs["library_source"])
                throw(new Error("Missing library_source attribute for Sample '" + sample.sample_name + "'"));
            if (!attrs["library_selection"])
                throw(new Error("Missing library_selection attribute for Sample '" + sample.sample_name + "'"));
            if (!attrs["library_layout"])
                throw(new Error("Missing library_layout attribute for Sample '" + sample.sample_name + "'"));
            if (!attrs["platform_type"])
                throw(new Error("Missing platform_type attribute for Sample '" + sample.sample_name + "'"));
            if (!attrs["platform_model"])
                throw(new Error("Missing platform_model attribute for Sample '" + sample.sample_name + "'"));

            let experimentAlias = "experiment_" + sample.sample_id + "_" + self.id;
            let experimentObj = {
                EXPERIMENT: {
                  $: { alias: experimentAlias },
                  TITLE: "",
                  STUDY_REF: { $: { accession: self.projectAccession } },
                  DESIGN: {
                    DESIGN_DESCRIPTION: {},
                    SAMPLE_DESCRIPTOR: { $: { accession: sampleAccession } },
                    LIBRARY_DESCRIPTOR: {
                      LIBRARY_STRATEGY: attrs["library_strategy"].toUpperCase(),
                      LIBRARY_SOURCE: attrs["library_source"].toUpperCase(),
                      LIBRARY_SELECTION: attrs["library_selection"],
                      LIBRARY_LAYOUT: {}
//                                        attrs["library_layout"]: {} // SINGLE or PAIRED
//                                      },
//                                      LIBRARY_CONSTRUCTION_PROTOCOL: "Messenger RNA (mRNA) was isolated using the Dynabeads mRNA Purification Kit (Invitrogen, Carlsbad Ca. USA) and then sheared using divalent cations at 72*C. These cleaved RNA fragments were transcribed into first-strand cDNA using II Reverse Transcriptase (Invitrogen, Carlsbad Ca. USA) and N6 primer (IDT). The second-strand cDNA was subsequently synthesized using RNase H (Invitrogen, Carlsbad Ca. USA) and DNA polymerase I (Invitrogen, Shanghai China). The double-stranded cDNA then underwent end-repair, a single `A? base addition, adapter ligati on, and size selection on anagarose gel (250 * 20 bp). At last, the product was indexed and PCR amplified to finalize the library prepration for the paired-end cDNA."
                    }
                  },
                  PLATFORM: {}
//                                    attrs["platform_type"]: { INSTRUMENT_MODEL: attrs["platform_model"] }
//                                  },
//                                  EXPERIMENT_ATTRIBUTES: {
//                                    EXPERIMENT_ATTRIBUTE: {
//                                      TAG: "library preparation date",
//                                      VALUE: "2010-08"
//                                    }
//                                  }
                }
            };
            experimentObj.EXPERIMENT.DESIGN.LIBRARY_DESCRIPTOR.LIBRARY_LAYOUT[attrs["library_layout"].toUpperCase()] = {}; // SINGLE or PAIRED
            experimentObj.EXPERIMENT.PLATFORM[attrs["platform_type"].toUpperCase()] = { INSTRUMENT_MODEL: attrs["platform_model"] };

            let runsObj = [];
            files.forEach(file => {
                let runAlias = "run_" + sample.sample_id + "_" + runsObj.length + "_" + self.id;
                //self.filesByAlias[runAlias] = file;
                runsObj.push({
                    RUN: {
                      $: { alias: runAlias },
                      EXPERIMENT_REF: { $: { refname: experimentAlias } },
                      DATA_BLOCK: {
                        FILES: {
                          FILE: {
                            $: {
                              filename: path.basename(file.get().newFile),
                              filetype: "fastq",
                              checksum_method: "MD5",
                              checksum: file.get().md5sum
                            }
                          }
                        }
                      }
                    }
                });
            });

            experimentSetObj.EXPERIMENT_SET.push(experimentObj);
            runSetObj.RUN_SET = runsObj;
        });

        let experimentXml = this.builder.buildObject(experimentSetObj);
        let runXml = this.builder.buildObject(runSetObj);
        return [ experimentXml, runXml ];
    }

    async submitProject(submissionXml, projectXml, sampleXml) {
        let self = this;

        let options = {
            method: "POST",
            uri: self.submissionUrl,
            headers: {
                "Authorization": "Basic " + new Buffer(self.username + ":" + self.password).toString('base64'),
                "Accept": "application/xml",
            },
            formData: {
                SUBMISSION: {
                    value: submissionXml, //fs.createReadStream(tmpPath + '__submission__.xml'),
                    options: {
                        filename: 'SUBMISSION.xml',
                        contentType: 'application/xml'
                    }
                },
                PROJECT: {
                    value: projectXml, //fs.createReadStream(tmpPath + '__project__.xml'),
                    options: {
                        filename: 'PROJECT.xml',
                        contentType: 'application/xml'
                    }
                },
                SAMPLE: {
                    value: sampleXml, //fs.createReadStream(tmpPath + '__sample__.xml'),
                    options: {
                        filename: 'SAMPLE.xml',
                        contentType: 'application/xml'
                    }
                },
            }
        };

        let parsedBody = await requestp(options);
        console.log(parsedBody);
        let response = await xmlToObj(parsedBody);
        console.log(response);

        if (response.RECEIPT.$.success == "false") {
            if (response.RECEIPT.MESSAGES) {
                response.RECEIPT.MESSAGES.forEach( message => {
                    if (message.ERROR) {
                        console.log(message.ERROR);
                        throw(new Error(message.ERROR.join(",")));
                    }
                });
            }
            else {
                throw(new Error("Unknown error"));
            }
        }

        return response;
    }

    async submitExperiments(submissionXml, experimentXml, runXml) {
        let self = this;

        let options2 = {
            method: "POST",
            uri: self.submissionUrl,
            headers: {
                "Authorization": "Basic " + new Buffer(self.username + ":" + self.password).toString('base64'),
                "Accept": "application/xml",
            },
            formData: {
                SUBMISSION: {
                    value: submissionXml, //fs.createReadStream(tmpPath + '__submission__.xml'),
                    options: {
                        filename: 'SUBMISSION.xml',
                        contentType: 'application/xml'
                    }
                },
                EXPERIMENT: {
                    value: experimentXml, //fs.createReadStream(tmpPath + '__experiment__.xml'),
                    options: {
                        filename: 'EXPERIMENT.xml',
                        contentType: 'application/xml'
                    }
                },
                RUN: {
                    value: runXml, //fs.createReadStream(tmpPath + '__run__.xml'),
                    options: {
                        filename: 'RUN.xml',
                        contentType: 'application/xml'
                    }
                }
            }
        };

        let parsedBody = await requestp(options2);
        console.log(parsedBody);
        let response = await xmlToObj(parsedBody);

        if (response.RECEIPT.$.success == "false") {
            if (response.RECEIPT.MESSAGES) {
                response.RECEIPT.MESSAGES.forEach( message => {
                    if (message.ERROR) {
                        console.log(message.ERROR);
                        throw(new Error(message.ERROR.join(",")));
                    }
                });
            }
            else {
                throw(new Error("Unknown error"));
            }
        }

//      if (response.RECEIPT.RUN) {
//          response.RECEIPT.RUN.forEach(run => {
//              let alias = run.$.alias;
//              let accession = run.$.accession;
//              filesByAlias[alias].dataValues.accession = accession;
//           });
//      }

        return response;
    }

    async submitRelease() {
        let self = this;

        let submissionXml = this.builder.buildObject({
            SUBMISSION: {
                ACTIONS: {
                    ACTION: {
                        RELEASE: { $: { target: self.projectAccession } }
                    }
                }
            }
        });
        console.log(submissionXml);

        let options2 = {
            method: "POST",
            uri: self.submissionUrl,
            headers: {
                "Authorization": "Basic " + new Buffer(self.username + ":" + self.password).toString('base64'),
                "Accept": "application/xml",
            },
            formData: {
                SUBMISSION: {
                    value: submissionXml,
                    options: {
                        filename: 'SUBMISSION.xml',
                        contentType: 'application/xml'
                    }
                }
            }
        };
        let parsedBody = await requestp(options2);
        console.log(parsedBody);
        let response = await xmlToObj(parsedBody);
        console.log(response);

        if (response.RECEIPT.$.success == "false") {
            if (response.RECEIPT.MESSAGES) {
                response.RECEIPT.MESSAGES.forEach( message => {
                    if (message.ERROR) {
                        console.log(message.ERROR);
                        throw(new Error(message.ERROR.join(",")));
                    }
                });
            }
            else {
                throw(new Error("Unknown error"));
            }
        }

        return response;
    }
}

function xmlToObj(xml) {
    return new Promise(function(resolve, reject) {
        xml2js.parseString(xml, function(err, result) {
            if (err)
                reject(err);
            else
                resolve(result);
        });
    });
}

exports.ENA = ENA;