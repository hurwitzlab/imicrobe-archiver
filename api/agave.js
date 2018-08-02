const https = require('https');
const fs = require('fs');
const config = require('../config.json');

class AgaveAPI {
    constructor(props) {
        this.token = props.token;
    }

    filesGet(remotePath, localPath) {
        var self = this;

        // Request file from Agave
        return new Promise(function(resolve, reject) {
            console.log("AgaveAPI.filesGet " + remotePath + " " + localPath);

            var options = {
                host: config.agaveConfig.baseUrl.replace(/^https?:\/\//,''), // remove protocol
                path: "/files/v2/media/" + remotePath,
                headers: {
                    Authorization: self.token
                }
            };

            var file = fs.createWriteStream(localPath);

            var request = https.get(options, response => {
                response.pipe(file);

                response.on("end", function() {
                    file.end();
                    resolve();
                });
            });

            request.on("error", function(error) {
                console.log("https error:", error);
                reject(error);
            });
        });
    }
}

exports.AgaveAPI = AgaveAPI;