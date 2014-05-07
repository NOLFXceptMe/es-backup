#!/usr/bin/env node

var program = require('commander');
var http = require('http');
var async = require('async');
var moment = require('moment');

program.version("0.0.1")
    .option('-r, --repository [repository]', 'Specify repository')
    .option('-s, --snapshot [snapshot]', 'Specify snapshot')
    .option('-b, --backup', 'Take backup')
    .option('-R, --register', 'Register repository')
    .option('-p, --path [path]', 'Repository path')
    .parse(process.argv);

var repo = program.repository;
var snapshot = program.snapshot;
var path = program.path;

async.waterfall([
    /* Check for repository */
    function(callback) {
        checkForRepository(callback);
    },


    /* Register if need be AND requested */
    function(isRegistered, callback) {
        if(!isRegistered && program.register) {
            registerRepository(callback);
        } else {
            callback(null, isRegistered);
        }
    },

    /* Take backup if requested */
    function(repoExists, callback) {
        if(repoExists && program.backup) {
            takeBackup(callback);
        } else {
            if(!repoExists) {
                console.error("Repository does not exist. Backup terminated");
            } else {
                console.log("No request for backup.");
                callback(null, false);
            }
        }
    }
], function(err, result) {
    main(result);
});

function main(result) {
    if(result) {
        console.log("Backup successfully completed.");
    } else {
        console.error("No backup taken.")
    }

    process.exit();
}

/* Check if the repository mentioned exists
* return true, or false*/
function checkForRepository(callback) {
    var flag = false;

    var options = {
        hostname : 'localhost',
        port : 9200,
        path : '/_snapshot/' + repo,
        method : 'GET'
    }

    var req = http.request(options, function(res) {
        if(res.statusCode == 404) {
            console.error("No repository '" + repo + "' is registered for ES backups.");
            flag = false;
        }

        if(res.statusCode == 200) {
            console.log("Repository found.");
            flag = true;
        }

        callback(null, flag);
    }).end();
}

function registerRepository(callback) {
    if(!path) {
        console.error("Cannot register repository without path");
        callback(null, false);
    }

    var options = {
        hostname : 'localhost',
        port : 9200,
        path : '/_snapshot/' + repo,
        method : 'PUT'
    }

    var default_repo_options = {
        type : 'fs',
        settings : {
            location : path,
            compress : true
        }
    };

    var req = http.request(options, function(res) {
        res.on('data', function(body) {
            var jsonBody = JSON.parse(body);

            if(jsonBody.acknowledged) {
                console.log('Created repository ' + repo + ' at ' + path);
                callback(null, true);
            }
            else
                callback(null, false);
        });
    });

    req.on('error', function(e) {
        console.error("Error while attempting to register repo");
        console.error(e);

        callback(null, false);
    });

    req.write(JSON.stringify(default_repo_options));
    req.end();
}

function takeBackup(callback) {
    if(!snapshot) {
        var date = moment().format('YYYY-MM-DD');
        snapshot = "esbackup." + date;
    }

    var options = {
        hostname : 'localhost',
        port : 9200,
        path : '/_snapshot/' + repo + '/' + snapshot + '?wait_for_completion=true',
        method : 'PUT'
    }

    console.log("Starting backup. Repository '" + repo + "' and snapshot '" + snapshot + "'");
    var req = http.request(options, function(res) {
        res.on('data', function(body) {
           var jsonBody = JSON.parse(body);
           console.log(jsonBody);
        });

        callback(null, true);
    }).on('error', function(e) {
        console.error("Error while taking ES backup");
        console.error(e);

        callback(null, false);
    }).end();
}