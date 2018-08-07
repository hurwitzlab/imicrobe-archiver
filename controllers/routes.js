'use strict';

const job  = require('../libs/job');
const cors = require('cors');
const bodyParser = require('body-parser');
const requestp = require('request-promise');
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
        requireAuth(request);

        var jobs = await jobManager.getJobs(request.auth.profile.username);
        if (jobs) {
            jobs = jobs.map(j => {
                j.inputs = arrayify(j.inputs);
                //j.owner = j.username;
                return j;
            });
        }

        response.json({
            status: "success",
            result: jobs || []
        });
    });

//    app.get('/jobs/:id(\\S+)', async (request, response) => {
    app.get('/jobs/:id([\\w\\-]+)', async (request, response) => {
        requireAuth(request);

        try {
            var job = await jobManager.getJob(request.params.id, request.auth.profile.username);
            if (!job)
                throw(ERR_NOT_FOUND);

            job.inputs = arrayify(job.inputs);
            //job.owner = job.username;
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
        j.token = request.auth.profile.token;
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

function agaveTokenValidator(req, res, next) {
    var token;
    if (req && req.headers)
        token = req.headers.authorization;
    console.log("validateAgaveToken: token:", token);

    req.auth = {
        validToken: false
    };

    if (!token)
        next();
    else {
        getAgaveProfile(token)
        .then(function (response) {
            if (!response || response.status != "success") {
                console.log('validateAgaveToken: !!!! Bad profile status: ' + response.status);
                return;
            }
            else {
                response.result.token = token;
                return response.result;
            }
        })
        .then( profile => {
            if (profile) {
                console.log("validateAgaveToken: *** success ***  username:", profile.username);

                req.auth = {
                    validToken: true,
                    profile: profile

                };
            }
        })
        .catch( error => {
            console.log("validateAgaveToken: !!!!", error.message);
        })
        .finally(next);
    }
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
