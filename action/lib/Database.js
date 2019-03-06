// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements; and to You under the Apache License, Version 2.0.

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
                    reject(err);
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

    this.updateTrigger = function(existing, params) {
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
        .then(() => this.getTrigger(existing.triggerName))
        .then(doc => {
            for (var key in params) {
                if (params[key] !== undefined) {
                    doc[key] = params[key];
                }
            }
            var status = {
                'active': true,
                'dateChanged': Date.now()
            };
            doc.status = status;

            return new Promise((resolve, reject) => {
                this.db.insert(doc, (err, result) => {
                    if(err) {
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            });
        });
    };
};
