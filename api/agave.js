const https = require('https');
const fs = require('fs');
const config = require('../config.json');

class AgaveAPI {
    constructor(props) {
        this.token = props.token;
    }

    filesGet(remotePath, localPath) {
        var self = this;

        var options = {
            host: config.agaveConfig.baseUrl.replace(/^https?:\/\//,''), // remove protocol
            path: "/files/v2/media/" + remotePath,
            headers: {
                //Accept: "application/octet-stream",
                Authorization: self.token
            }
        }

        var file = fs.createWriteStream(localPath);

        // Request file from Agave
        try {
            console.log("AgaveAPI.filesGet " + remotePath + " " + localPath);
            https.get(options, response => {
                response.pipe(file);
            });
        }
        catch(error) {
            console.log(error);
            res.send(500, error)
        }
    }
}

exports.AgaveAPI = AgaveAPI;