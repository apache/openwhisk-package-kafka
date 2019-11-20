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

// constructor for DB object - a thin, promise-loving wrapper around nano
module.exports = function(dbURL, dbName) {
    var nano = require('nano')(dbURL);
    this.db = nano.db.use(dbName);

    const designDoc = "filters";
    const assignmentView = "by-worker";

    this.getTrigger = function(triggerFQN) {
        return new Promise((resolve, reject) => {
            this.db.get(triggerFQN, (err, result) => {
                if(err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    };

    this.ensureTriggerIsUnique = function(triggerFQN) {
        return this.getTrigger(this.db, triggerFQN)
            .then(result => {
                return Promise.reject('Trigger already exists');
            })
            .catch(err => {
                // turn that frown upside-down!
                return true;
            });
    };

    this.recordTrigger = function(params) {
        console.log('recording trigger');

        params['_id'] = params.triggerName;
        params['status'] = {
            'active': true,
            'dateChanged': Date.now()
        };

        return new Promise((resolve, reject) => {
            this.db.insert(params, (err, result) => {
                if(err) {
                    if(err.statusCode && err.statusCode === 409) {
                        this.getTrigger(params.triggerName)
                        .then(doc => this.disableTrigger(doc))
                        .then(() => this.getTrigger(params.triggerName))
                        .then(doc => this.updateTrigger(params, {_rev: doc._rev}))
                        .then(result => resolve(result))
                        .catch(err => reject(err));
                    } else {
                        reject(err);
                    }
                } else {
                    resolve(result);
                }
            });
        });
    };

    this.deleteTrigger = function(triggerFQN) {
        return this.getTrigger(triggerFQN)
            .then(doc => {
                return new Promise((resolve, reject) => {
                    this.db.destroy(doc._id, doc._rev, (err, result) => {
                        if(err) {
                            reject(err);
                        } else {
                            resolve(result);
                        }
                    });
                });
            })
    };

    this.getTriggerAssignment = function(workers) {

        return new Promise((resolve, reject) => {
            var assignment = workers[0] || 'worker0';

            if (workers.length > 1) {
                this.db.view(designDoc, assignmentView, {group: true}, (err, result) => {
                    if (err) {
                        reject(err);
                    } else {
                        // a map between available workers and their number of assigned triggers
                        // values will be populated with the results of the assignment view
                        var counter = {};
                        workers.forEach(worker => {
                            counter[worker] = 0;
                        });

                        // update counter values with the number of assigned triggers
                        // for each worker
                        result.rows.forEach(row => {
                            if (row.key in counter) {
                                counter[row.key] = row.value;
                            }
                        });

                        // find which of the available workers has the least number of
                        // assigned triggers
                        for (availableWorker in counter) {
                            if (counter[availableWorker] < counter[assignment]) {
                                assignment = availableWorker;
                            }
                        }
                        resolve(assignment);
                    }
                });
            } else {
                resolve(assignment);
            }
        });
    };

    this.disableTrigger = function(existing) {
        return new Promise((resolve, reject) => {
            var message = 'Automatically disabled trigger while updating';
            var status = {
                'active': false,
                'dateChanged': Date.now(),
                'reason': {'kind': 'AUTO', 'statusCode': undefined, 'message': message}
            };
            existing.status = status;
            this.db.insert(existing, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        })
    };

    this.updateTrigger = function(existing, params) {
        for (var key in params) {
            if (params[key] !== undefined) {
                existing[key] = params[key];
            }
        }
        var status = {
            'active': true,
            'dateChanged': Date.now()
        };
        existing.status = status;

        return new Promise((resolve, reject) => {
            this.db.insert(existing, (err, result) => {
                if(err) {
                    reject(err);
                } else {
                    resolve(result);
                }
            });
        });
    };
};
