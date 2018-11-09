'use strict';

const cors = require('cors');
const bodyParser = require('body-parser');
const requestp = require('request-promise');
const job  = require('../libs/job');
const config = require('../config.json');

// Create error types
class MyError extends Error {
    constructor(message, statusCode) {
        super(message);
        this.statusCode = statusCode;
    }
}

const ERR_BAD_REQUEST = new MyError("Bad request", 400);
const ERR_UNAUTHORIZED = new MyError("Unauthorized", 401);
const ERR_PERMISSION_DENIED = new MyError("Permission denied", 403);
const ERR_NOT_FOUND = new MyError("Not found", 404);

module.exports = function(app, jobManager) {
    app.use(cors());
    app.use(bodyParser.json()); // support json encoded bodies
    app.use(bodyParser.urlencoded({ extended: true })); // support encoded bodies

    app.use(requestLogger);
    app.use(agaveTokenValidator);

    app.get('/jobs', async (request, response) => {
        try {
            requireAuth(request);

            var jobs = await jobManager.getJobs(request.auth.profile.username);

            response.json({
                status: "success",
                result: jobs || []
            });
        }
        catch(error) {
            errorHandler(error, request, response);
        }
    });

    app.get('/jobs/:id([\\w\\-]+)', async (request, response) => {
        try {
            requireAuth(request);

            var job = await jobManager.getJob(request.params.id, request.auth.profile.username);
            if (!job)
                throw(ERR_NOT_FOUND);

            response.json({
                status: "success",
                result: job
            });
        }
        catch(error) {
            errorHandler(error, request, response);
        };
    });

    app.get('/jobs/project/:id([\\w\\-]+)', async (request, response) => {
        try {
            requireAuth(request);

            var job = await jobManager.getJobByProjectId(request.params.id, request.auth.profile.username);
            if (!job)
                throw(ERR_NOT_FOUND);

            response.json({
                status: "success",
                result: job
            });
        }
        catch(error) {
            errorHandler(error, request, response);
        };
    });

    app.get('/jobs/:id([\\w\\-]+)/history', async (request, response) => {
        try {
            requireAuth(request);

            var job = await jobManager.getJob(request.params.id, request.auth.profile.username);
            if (!job)
                throw(ERR_NOT_FOUND);

            //var history = arrayify(job.history); // TODO
            response.json({
                status: "success",
                result: []
            });
        }
        catch(error) {
            errorHandler(error, request, response);
        };
    });

    app.post('/jobs', async (request, response) => {
        var j = new job.Job(request.body);
        j.username = request.auth.profile.username;
        await jobManager.submitJob(j);

        response.json({
            status: "success",
            result: {
                id: j.id
            }
        });
    });

    app.use(errorHandler);

    // Catch-all function
    app.get('*', function(req, res, next){
        res.status(404).send("Unknown route: " + req.path);
    });
}

function requestLogger(req, res, next) {
    console.log(["REQUEST:", req.method, req.url].join(" ").concat(" ").padEnd(80, "-"));
    next();
}

function errorHandler(error, req, res, next) {
    console.log("ERROR ".padEnd(80, "!"));
    console.log(error.stack);

    let statusCode = error.statusCode || 500;
    let message = error.message || "Unknown error";

    res.status(statusCode).send(message);
}

function requireAuth(req) {
    if (!req || !req.auth || !req.auth.validToken || !req.auth.profile)
        throw(ERR_UNAUTHORIZED);
}

async function agaveTokenValidator(req, res, next) {
    var token = (req && req.headers) ? req.headers.authorization : null;
    console.log("validateAgaveToken: token:", token);

    // Create auth object and default to unauthorized
    req.auth = {
        validToken: false
    };

    if (token) {
        try {
            var response = await getAgaveProfile(token);
            if (!response || response.status != "success") {
                console.log('validateAgaveToken: !!!! Bad profile status: ' + response.status);
                return;
            }

            var profile = response.result;
            if (profile) {
                console.log("validateAgaveToken: *** success ***  username:", profile.username);

                // Add token to profile and add auth object to request
                profile.token = token;
                req.auth = {
                    validToken: true,
                    profile: profile
                };
            }
        }
        catch(error) {
            console.log("validateAgaveToken: !!!!", error.message);
            return;
        }
    }

    next();
}

function getAgaveProfile(token) {
    return requestp({
        method: "GET",
        uri: "https://agave.iplantc.org/profiles/v2/me", // FIXME hardcoded
        headers: {
            Authorization: token,
            Accept: "application/json"
        },
        json: true
    });
}

function arrayify(obj) {
    var newObj = {};
    Object.keys(obj).forEach(prop => {
        if (Array.isArray(obj))
            newObj[prop] = obj[prop];
        else
            newObj[prop] = [ obj[prop] ];
    });
    return newObj;
}
