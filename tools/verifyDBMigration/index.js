/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var shell = require('shelljs');

// will be populated by processArgs()
var cloudant,
    cloudant_user,
    cloudant_pass,
    db_name;

function processArgs() {
    if(process.argv.length != 5) {
        console.log('USAGE: node index.js CLOUDANT_USER CLOUDANT_PASS DB_NAME');
        process.exit(1);
    } else {
        cloudant_user = process.argv[2];
        cloudant_pass = process.argv[3];
        db_name = process.argv[4];

        const Cloudant = require('cloudant');
        cloudant = Cloudant({
            url: `https://${cloudant_user}:${cloudant_pass}@${cloudant_user}.cloudant.com`,
            plugin: 'promises'
        });
    }
}

function verifyDBMigration() {
    return verifyDBCreatedWithDesignDoc("container0", false)
        .then(() => {
            return verifyDBCreatedWithDesignDoc("container1", true);
        })
        .catch(err => console.error(`Failed to validate migration: ${JSON.stringify(err)}`));
}

function verifyDBCreatedWithDesignDoc(containerName, letContainerCreateDB) {
    return destroyDBIfNeeded()
        .then(() => {
            if(!letContainerCreateDB) {
                console.log(`Creating DB`);
                return cloudant.db.create(db_name)
            } else {
                console.log(`Letting the container create the DB`);
            }
        })
        .then(() => {
            console.log(`Firing up the docker container`);
            return startDockerContainer(containerName)
        })
        .then(() => {
            return verifyView();
        });
}

function destroyDBIfNeeded() {
    console.log('destroying db');
    return cloudant.db.list()
        .then(existingDBs => {
            if(existingDBs.indexOf(db_name) >= 0) {
                console.log(`${db_name} already exists - DESTROY!`);
                return cloudant.db.destroy(db_name);
            }
        });
}

function startDockerContainer(containerName) {
    var dockerStartStopPromise = new Promise((resolve, reject) => {
        var returnCode = shell.exec(`docker run -d --name ${containerName} -e CLOUDANT_USER=${cloudant_user} -e CLOUDANT_PASS=${cloudant_pass} openwhisk/kafkaprovider`).code

        if(returnCode != 0) {
            reject(`Failed to start docker container: ${returnCode}`);
            return;
        }

        console.log("Giving the container some time to start up...");
        setTimeout(function() {
            console.log("Stopping the container");
            var returnCode = shell.exec(`docker stop ${containerName}`).code;
            if(returnCode != 0) {
                reject('Failed to stop docker container');
                return;
            }

            console.log("Deleting the container");
            returnCode = shell.exec(`docker rm ${containerName}`).code;
            if(returnCode != 0) {
                reject('Failed to delete container');
                return;
            }

            resolve();
        }, 20000);
    });

    return dockerStartStopPromise;
}

function verifyView() {
    var db = cloudant.db.use(db_name);

    console.log('Verifying view exists and works as expected');
    return ensureViewReturns(db, 0)
        .then(() => {
            return db.insert({
                triggerURL: 'this is the only property needed by the view'
            });
        })
        .then(() => {
            // give it a few extra seconds to make sure the view is indexed
            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    ensureViewReturns(db, 1)
                        .then(resolve)
                        .catch(reject);
                }, 3000);
            });
        });
}

function ensureViewReturns(db, expectedNumberOfRows) {
    return db.view('filters', 'only-triggers', {include_docs: false})
        .then(results => {
            if(results.rows.length != expectedNumberOfRows) {
                return Promise.reject(`Expected view to contain ${expectedNumberOfRows} rows but got ${results.rows.length}`);
            }
        });
}

processArgs();
verifyDBMigration()
    .then(() => console.log('done!'))
    .catch(err => console.error(JSON.stringify(err)));
