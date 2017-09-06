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
            'dateChanged': Math.round(new Date().getTime() / 1000)
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
        // a map between available workers and their number of assigned triggers
        // values will be populated with the results of the assignment view
        console.log(`Available workers: ${JSON.stringify(workers, null, 2)}`);
        var counter = {};
        workers.forEach(worker => {
            counter[worker] = 0;
        });

        return new Promise((resolve, reject) => {
            this.db.view(designDoc, assignmentView, {group: true}, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    var assignment = workers[0] || 'worker0';

                    // update counter values with the number of assigned triggers
                    // for each worker
                    result.rows.forEach(row => {
                        counter[row.key] = row.value;
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
        });
    };
};
