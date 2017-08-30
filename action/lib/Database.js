// constructor for DB object - a thin, promise-loving wrapper around nano
module.exports = function(dbURL, dbName) {
    var _ = require('lodash');
    var nano = require('nano')(dbURL);
    this.db = nano.db.use(dbName);

    var DDOC_ID = "workers";
    var VIEW_ID = "triggers_by_workers";

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

    this.getWorkerAssignment = function(workers) {
        return new Promise((resolve, reject) => {
            this.db.view(DDOC_ID, VIEW_ID, {group: true}, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    var rows = result.rows;
                    var difference = _.difference(workers, rows.map(a => a.key));
                    if (difference.length) {
                        resolve(difference.shift());
                    } else {
                        var sorted = rows.sort((a, b) => a.value - b.value).map(a => a.key);
                        resolve(sorted.shift() || 'worker0');
                    }
                }
            });
        });
    };
};
