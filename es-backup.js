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
    .parse(process.argv);

var repo = program.repository;
var snapshot = program.snapshot;

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
            console.log("Either repo doesn't exist or no request for backup");
            callback(null, false);
        }
    }
], function(err, result) {
    main(result);
});

function main(result) {
    console.log("Backup : " + result);
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
            console.error("No repository '" + repo + "' is registered for ES backups. Use -R, --register to register");
        }

        if(res.statusCode == 200) {
            console.log("Repository found");
            flag = true;
        }

        callback(null, flag);
    }).end();
}

function registerRepository(callback) {
        var options = {
            hostname : 'localhost',
            port : 9200,
            path : '/_snapshot/' + repo,
            method : 'PUT'
        }

        var default_repo_options = {
            type : 'fs',
            settings : {
                location : '/home/naveen/es_backup',
                compress : true
            }
        };

        var req = http.request(options, function(res) {
            res.on('data', function(body) {
                var jsonBody = JSON.parse(body);
                console.log(jsonBody.acknowledged);

                if (jsonBody.acknowledged == true) {
                    flag = true;
                }
            });

            callback(null, true);
        });

        req.on('error', function(e) {
            console.error("Error connecting to ES while attempting to register repo");
            console.error(e);

            callback(null, false);
        });

        req.write(JSON.stringify(default_repo_options));
        req.end();
}

function takeBackup(callback) {
    if(!snapshot) {
        var date = moment().format('YYYY-MM-DD');
        snapshot = "ESBackup" + date;
    }

    var options = {
        hostname : 'localhost',
        port : 9200,
        path : '/_snapshot/' + repo + '/' + snapshot + '?wait_for_completion=true',
        method : 'PUT'
    }

    console.log("Making snapshot request");
    var req = http.request(options, function(res) {
        res.on('data', function(body) {
            flag = true;
           var jsonBody = JSON.parse(body);
            console.log(jsonBody);
        });

        callback(null, true);
    }).on('error', function(e) {
        console.error("Error in taking ES backup");
        console.error(e);

        callback(null, false);
    }).end();
}