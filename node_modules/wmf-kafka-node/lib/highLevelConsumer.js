'use strict';

var util = require('util'),
    _ = require('lodash'),
    events = require('events'),
    Client = require('./client'),
    protocol = require('./protocol'),
    uuid = require('cassandra-uuid').Uuid,
    common = require('./common'),
    Offset = require('./offset'),
    async = require("async"),
    errors = require('./errors'),
    zk = require('./zookeeper'),
    ZookeeperConsumerMappings = zk.ZookeeperConsumerMappings,
    retry = require('retry'),
    debug = require('debug')('kafka-node:HighLevelConsumer');

var DEFAULTS = {
    groupId: 'kafka-node-group',
    // Auto commit config
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    paused: false,
    maxNumSegments: 1000,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024,
    maxTickMessages: 1000,
    fromOffset: false,
    // If offset to consume from is out of range, the
    // offset will be reset to based on this value.
    // If -1, the offset will be reset to latest offset.
    // If -2, the offset will be reset to earliest value.
    // OTTO: TODO: the default SHOULD be -1, but the previous
    // behavior was -2.  Should we leave it at -2?
    autoOffsetReset: protocol.OFFSET_LARGEST
};

var HighLevelConsumer = function (client, topics, options) {
    if (!topics) {
        throw new Error('Must have payloads');
    }
    this.fetchCount = 0;
    this.client = client;
    this.options = _.defaults((common.validateConsumerOptions(options || {})), DEFAULTS);
    this.initialised = false;
    this.ready = false;
    this.paused = this.options.paused;
    this.rebalancing = false;
    this.id = this.options.id || this.options.groupId + '_' + uuid.random();
    this.payloads = this.buildPayloads(topics);
    this.topicPayloads = this.buildTopicPayloads(topics);
    this.topicList = [];
    this.connect();
};
util.inherits(HighLevelConsumer, events.EventEmitter);

HighLevelConsumer.prototype._genTopicList = function() {
    return this.topicPayloads.map(function(item) {
        return item.topic;
    }).filter(function(item) { return !!item; }).sort();
};

HighLevelConsumer.prototype.buildPayloads = function (payloads) {
    var self = this;
    return payloads.map(function (p) {
        if (typeof p !== 'object') p = { topic: p };
        p.partition = p.partition || 0;
        p.offset = p.offset || 0;
        p.maxBytes = self.options.fetchMaxBytes;
        p.metadata = 'm'; // metadata can be arbitrary
        return p;
    });
};

// Initially this will always be empty - until after a re-balance
HighLevelConsumer.prototype.buildTopicPayloads = function (topics) {
    var topicNames = [];
    topics.forEach(function (j) {
        if (topicNames.lastIndexOf(j.topic) === -1) {
            topicNames.push(j.topic);
        }
    });
    return topicNames.map(function(t) {
        return { topic: t };
    });
};

// Provide the topic payloads if requested
HighLevelConsumer.prototype.getTopicPayloads = function () {
    if (!this.rebalancing) return this.topicPayloads;
    return null;
};

HighLevelConsumer.prototype._signalTopicListChange = function() {
    if (this.rebalancing) {
        return;
    }
    var currTopics = this._genTopicList();
    // compare the two lists
    if (currTopics.length !== this.topicList.length) {
        this.topicList = currTopics;
        return this.emit('topics_changed', currTopics);
    }
    for (var idx = 0; idx < this.topicList.length; idx++) {
        if (currTopics.indexOf(this.topicList[idx]) === -1) {
            this.topicList = currTopics;
            return this.emit('topics_changed', currTopics);
        }
    }
    for (var idx = 0; idx < currTopics.length; idx++) {
        if (this.topicList.indexOf(currTopics[idx]) === -1) {
            this.topicList = currTopics;
            return this.emit('topics_changed', currTopics);
        }
    }
};

HighLevelConsumer.prototype.connect = function () {
    var self = this;
    //Client alreadyexists
    if (this.client.ready)
        this.init();

    this.client.on('ready', function () {
        if (!self.initialised) self.init();

        // Check the topics exist and create a watcher on them
        var topics = self.payloads.map(function (p) {
            return p.topic;
        })

        self.client.topicExists(topics, function (err) {
            if (err) {
                return self.emit('error', err);
            }
            self.initialised = true;
        });
    });

    // Wait for the consumer to be ready
    this.on('rebalanced', function () {
        self.fetchOffset(self.topicPayloads, function (err, topics) {
            if (err) {
                return self.emit('error', err);
            }

            self.ready = true;
            self.updateOffsets(topics, true);
            self.fetch();
            self._signalTopicListChange();
        });
    });

    function rebalance() {

        if (!self.rebalancing) {
            deregister();

            self.emit('rebalancing');

            self.rebalancing = true;
            // Nasty hack to retry 3 times to re-balance - TBD fix this
            var oldTopicPayloads = self.topicPayloads;
            var operation = retry.operation({
                retries: 10,
                factor: 2,
                minTimeout: 1 * 100,
                maxTimeout: 1 * 1000,
                randomize: true
            });

            operation.attempt(function (currentAttempt) {
                self.rebalanceAttempt(oldTopicPayloads, function (err) {
                    if (operation.retry(err)) {
                        return;
                    }
                    if (err) {
                        self.rebalancing = false;
                        return self.emit('error', new errors.FailedToRebalanceConsumerError(operation.mainError().toString()));
                    }
                    else {
                        var topicNames = self.topicPayloads.map(function (p) {
                            return p.topic;
                        });
                        self.client.refreshMetadata(topicNames, function (err) {
                            register();
                            self.rebalancing = false;
                            if (err) {
                              self.emit('error', err);
                            } else {
                              self.emit('rebalanced');
                            }
                        });
                    }
                });
            });
        }
    }

    function checkFreePartitions() {
        self.client.zk.getFreePartitions(self.options.groupId, function(err, list) {
            if (err) {
                self.emit('error', new Error(err.toString()));
                return;
            }
            if (!list || !list.length) {
                // all of the partitions have owners
                return;
            }
            var newTopicPayloads = [];
            // try to grab each partition in a probablistic manner
            for (var idx = 0; idx < list.length; idx++) {
                if (Math.floor(1000 * Math.random()) <= Math.floor(1000.0 / list[idx].noConsumers)) {
                    newTopicPayloads.push({ topic: list[idx].topic, partition: list[idx].partition, offset: 0, maxBytes: self.options.fetchMaxBytes, metadata: 'm'});
                }
            }
            // give up if no partitions have been selected
            // or the consumer has entered rebalancing
            if (!newTopicPayloads.length || self.rebalancing) {
                return;
            }
            // the consumer is now rebalancing, signal that
            deregister();
            self.emit('rebalancing');
            self.rebalancing = true;
            async.mapSeries(newTopicPayloads, function(tp, callback) {
                debug("HighLevelConsumer %s gaining ownership of %s", self.id, JSON.stringify(tp));
                self.client.zk.addPartitionOwnership(self.id, self.options.groupId, tp.topic, tp.partition, function (err) {
                    if (err) {
                        return callback();
                    }
                    return callback(null, tp);
                });
            }, function(err, addedList) {
                if (err) {
                    self.rebalancing = false;
                    return self.emit('error', new Error(err.toString()));
                }
                self.topicPayloads = addedList.filter(function(item) { return !!item; });
                var topicNames = self.topicPayloads.map(function (p) {
                    return p.topic;
                });
                self.client.refreshMetadata(topicNames, function (err) {
                    register();
                    self.rebalancing = false;
                    if (err) {
                        self.emit('error', err);
                    } else {
                        self.emit('rebalanced');
                    }
                });
            });
        });
    }

    // Check partition ownership
    function checkPartitionOwnership() {
        if (self.rebalancing) {
            return;
        }
        if (!self.topicPayloads.length) {
            // the consumer owns no partition
            // check whether there are partitions without an owner
            return checkFreePartitions();
        }
        if (!self.topicPayloads.some(function(tp) { return tp.partition !== undefined; })) {
            // no partitions assigned, so nothing to check
            return;
        }
        // the process owns some partitions, check the ownership
        var payloads = [];
        async.each(self.topicPayloads, function (tp, cbb) {
            if (!tp.partition) {
                return cbb();
            }
            self.client.zk.getPartitionOwnership(self.options.groupId, tp.topic, tp.partition, function (err, ownerId) {
                if (err) {
                    return cbb(err);
                }
                if (self.id === ownerId) {
                    // the partition is still owned by this consumer
                    payloads.push(tp);
                }
                cbb();
            });
        }, function (err) {
            if (err) {
                return self.emit('error', new errors.FailedToRegisterConsumerError(err.toString()));
            }
            if (!self.rebalancing) {
                self.topicPayloads = payloads;
                self._signalTopicListChange();
            }
        });
    }
    this.checkPartitionOwnershipInterval = setInterval(checkPartitionOwnership, 20000);

    // TEMP TEMP TEMP
    // forced rebalance
    this.forcedRebalanceInterval = setTimeout(rebalance,
        Math.floor(Math.random() * 3 * 60 * 1000) + 7 * 60 * 1000);

    // Wait for the consumer to be ready
    this.on('registered', function () {
        rebalance();
    });

    function register() {
        debug("Registered listeners");
        // Register for re-balances (broker or consumer changes)
        self.client.zk.on('consumersChanged', rebalance);
        self.client.on('brokersChanged', rebalance);
    }

    function deregister() {
        debug("Deregistered listeners");
        // Register for re-balances (broker or consumer changes)
        self.client.zk.removeListener('consumersChanged', rebalance);
        self.client.removeListener('brokersChanged', rebalance);
    }

    function attachZookeeperErrorListener() {
        self.client.zk.on('error', function (err) {
            self.emit('error', err);
        });
    }

    attachZookeeperErrorListener();

    this.client.on('zkReconnect', function () {
        attachZookeeperErrorListener();

        if(self.checkPartitionOwnershipInterval) {
            clearInterval(self.checkPartitionOwnershipInterval);
        }
        if(self.forcedRebalanceInterval) {
            clearInterval(self.forcedRebalanceInterval);
        }

        self.registerConsumer(function () {
            rebalance();
            self.checkPartitionOwnershipInterval = setInterval(checkPartitionOwnership, 20000);
            self.forcedRebalanceInterval = setTimeout(rebalance,
                Math.floor(Math.random() * 3 * 60 * 1000) + 7 * 60 * 1000);
        });
    });

    this.client.on('error', function (err) {
        self.emit('error', err);
    });

    this.client.on('reconnect', function(lastError){
        self.fetch();
    })

    this.client.on('close', function () {
        debug('close');
    });

    // In case of offsetOutOfRange error when fetching,
    // reset this consumer's topic-partition offset to
    // a value based on self.options.autoOffsetReset.
    this.on('offsetOutOfRange', self.resetOffset.bind(self));

    // 'done' will be emit when a message fetch request complete
    this.on('done', function (topics) {
        self.updateOffsets(topics);
        if (!self.paused) {
            setImmediate(function () {
                self.fetch();
            });
        }
    });
};

/**
 * Issues an offset request and calls setOffset with the returned value.
 * If topPar.time is not given, it will be set to the value of
 * self.options.autoOffsetReset. topPar shoould be of the form
 *  {
 *      topic: 't',
 *      partition: 0,
 *      time: ... # optional
 *  }
 */
HighLevelConsumer.prototype.resetOffset = common.resetOffset;

HighLevelConsumer.prototype.rebalanceAttempt = function (oldTopicPayloads, cb) {
    var self = this;
    // Do the rebalance.....
    var consumerPerTopicMap;
    var newTopicPayloads = [];
    var finalTopicPayloads = [];
    debug("HighLevelConsumer %s is attempting to rebalance", self.id);
    async.series([

            // Stop fetching data and commit offsets
            function (callback) {
                debug("HighLevelConsumer %s stopping data read during rebalance", self.id);
                self.stop(function () {
                    callback();
                })
            },

            // Assemble the data
            function (callback) {
                debug("HighLevelConsumer %s assembling data for rebalance", self.id);
                self.client.zk.getConsumersPerTopic(self.options.groupId, function (err, obj) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        consumerPerTopicMap = obj;
                        callback();
                    }
                });
            },

            // Rebalance
            function (callback) {
                debug("HighLevelConsumer %s determining the partitions to own during rebalance", self.id);
                for (var topic in consumerPerTopicMap.consumerTopicMap[self.id]) {
                    var topicToAdd = consumerPerTopicMap.consumerTopicMap[self.id][topic];
                    var numberOfConsumers = consumerPerTopicMap.topicConsumerMap[topicToAdd].length;
                    var numberOfPartitions = consumerPerTopicMap.topicPartitionMap[topicToAdd].length;
                    var partitionsPerConsumer = Math.floor(numberOfPartitions / numberOfConsumers);
                    var extraPartitions = numberOfPartitions % numberOfConsumers;
                    var currentConsumerIndex;
                    if (numberOfPartitions >= numberOfConsumers) {
                        // there are more partitions than consumers, so assign
                        // multiple partitions to this consumer
                        for (var index in consumerPerTopicMap.topicConsumerMap[topicToAdd]) {
                            if (consumerPerTopicMap.topicConsumerMap[topicToAdd][index] === self.id) {
                                currentConsumerIndex = parseInt(index);
                                break;
                            }
                        }
                        var extraBit = currentConsumerIndex;
                        if (currentConsumerIndex > extraPartitions) extraBit = extraPartitions
                        var startPart = partitionsPerConsumer * currentConsumerIndex + extraBit;
                        var extraNParts = 1;
                        if (currentConsumerIndex + 1 > extraPartitions) extraNParts = 0;
                        var nParts = partitionsPerConsumer + extraNParts;
                        for (var i = startPart; i < startPart + nParts; i++) {
                            newTopicPayloads.push({ topic: topicToAdd, partition: consumerPerTopicMap.topicPartitionMap[topicToAdd][i], offset: 0, maxBytes: self.options.fetchMaxBytes, metadata: 'm'});
                        }
                    } else {
                        // there are more consumers subscribed than partitions available,
                        // so pick a subset of them to own in a probablistic manner
                        for (var index = 0; index < numberOfPartitions; index++) {
                            if (Math.floor(1000 * Math.random()) <= Math.floor(1000.0 / numberOfConsumers)) {
                                newTopicPayloads.push({ topic: topicToAdd, partition: consumerPerTopicMap.topicPartitionMap[topicToAdd][index], offset: 0, maxBytes: self.options.fetchMaxBytes, metadata: 'm'});
                            }
                        }
                    }
                }
                callback();
            },

            // Update ZK with new ownership
            function (callback) {
                var errs = [];
                var tmpPayloads = [];
                if (!newTopicPayloads.length) {
                    return callback();
                }
                async.eachSeries(newTopicPayloads, function (tp, cbb) {
                    if (tp['partition'] === undefined) {
                        // nothing to do, continue
                        return cbb();
                    }
                    debug("HighLevelConsumer %s gaining ownership of %s", self.id, JSON.stringify(tp));
                    self.client.zk.addPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                        if (err) {
                            errs.push({topic: tp, error: err});
                            return cbb();
                        }
                        tmpPayloads.push(tp);
                        cbb();
                    });
                }, function (err) {
                    if (err) {
                        return callback(err);
                    }
                    if (errs.length && !finalTopicPayloads.length) {
                        // no new topic partition added, but there were errors
                        return callback('No new partition added due to errors: ' + JSON.stringify(errs));
                    }
                    newTopicPayloads = tmpPayloads;
                    callback();
                });
            },

            // check the old and new partitions (again)
            function (callback) {
                newTopicPayloads = oldTopicPayloads.concat(newTopicPayloads);
                async.eachSeries(newTopicPayloads, function(tp, cbb) {
                    if (tp['partition'] === undefined) {
                        return cbb();
                    }
                    self.client.zk.checkPartitionOwnership(self.id, self.options.groupId, tp['topic'], tp['partition'], function (err) {
                        if (!err) {
                            // no error - this client is still the owner
                            finalTopicPayloads.push(tp);
                        }
                        cbb();
                    });
                }, function(err) {
                    if (err) {
                        return callback(err);
                    }
                    callback();
                });
            },

            // Update the new topic offsets
            function (callback) {
                // get rid of duplicates
                newTopicPayloads = [];
                for (var idx = 0; idx < finalTopicPayloads.length; idx++) {
                    var found = false;
                    for(var jdx = idx + 1; jdx < finalTopicPayloads.length; jdx++) {
                        if (finalTopicPayloads[idx].topic === finalTopicPayloads[jdx].topic &&
                                finalTopicPayloads[idx].partition === finalTopicPayloads[jdx].partition) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        newTopicPayloads.push(finalTopicPayloads[idx]);
                    }
                }
                self.topicPayloads = newTopicPayloads;
                callback();
            }],
        function (err) {
            if (err) {
                debug("HighLevelConsumer %s rebalance attempt failed", self.id);
                cb(err);
            }
            else {
                debug("HighLevelConsumer %s rebalance attempt was successful", self.id);
                cb();
            }
        });
};

HighLevelConsumer.prototype.init = function () {

    var self = this;

    if (!self.topicPayloads.length) {
        return;
    }

    self.registerConsumer(function (err) {
        if (err) {
            return self.emit('error', new errors.FailedToRegisterConsumerError(err.toString()));
        }

        // Close the
        return self.emit('registered');
    });
};

/*
 * Update offset info in current payloads
 * @param {Object} Topic-partition-offset
 * @param {Boolean} Don't commit when initing consumer
 */
HighLevelConsumer.prototype.updateOffsets = function (topics, initing) {
    var self = this;
    this.topicPayloads.forEach(function (p) {
        if (!_.isEmpty(topics[p.topic]) && topics[p.topic][p.partition] !== undefined) {
            var offset = topics[p.topic][p.partition];
            if (!self.options.autoOffsetReset && offset === -1) offset = 0;
            if (!initing) p.offset = offset + 1;
            else p.offset = offset;
        }
    });

    if (this.options.autoCommit && !initing) {
        this.autoCommit(false, function (err) {
            err && debug('auto commit offset', err);
        });
    }
};

function autoCommit(force, cb) {
    if (arguments.length === 1) {
        cb = force;
        force = false;
    }

    if (this.committing && !force) return cb(null, 'Offset committing');

    this.committing = true;
    setTimeout(function () {
        this.committing = false;
    }.bind(this), this.options.autoCommitIntervalMs);

    var commits = this.topicPayloads.filter(function (p) { return p.offset !== 0 });

    if (commits.length) {
        this.client.sendOffsetCommitRequest(this.options.groupId, commits, cb);
    } else {
        cb(null, 'Nothing to be committed');
    }
}
HighLevelConsumer.prototype.commit = HighLevelConsumer.prototype.autoCommit = autoCommit;

HighLevelConsumer.prototype.fetch = function () {
    if (!this.ready || this.rebalancing || this.paused) {
        return;
    }

    this.client.sendFetchRequest(this, this.topicPayloads, this.options.fetchMaxWaitMs, this.options.fetchMinBytes, this.options.maxTickMessages);
};

HighLevelConsumer.prototype.fetchOffset = function (payloads, cb) {
    var self = this;
    this.client.sendOffsetFetchRequest(this.options.groupId, payloads, function (e, offsetFetchResponse) {

        if (!self.options.autoOffsetReset) return cb(e, offsetFetchResponse);

        var latest = [];
        Object.keys(offsetFetchResponse).forEach(function (topicName) {
            var topic = offsetFetchResponse[topicName];
            Object.keys(topic).forEach(function (partition) {
                if (topic[partition] === -1) {
                    latest.push({
                        topic: topicName,
                        partition: partition,
                        time: self.options.autoOffsetReset,
                        maxNum: 1,
                        metadata: 'm'});
                }
            });
        });

        if (!latest.length) return cb(e, offsetFetchResponse);

        self.client.sendOffsetRequest(latest, function (e, offsetResponse) {
            Object.keys(offsetResponse).forEach(function (topicName) {
                var topic = offsetResponse[topicName];
                Object.keys(topic).forEach(function (partition) {
                    offsetFetchResponse[topicName][partition] = topic[partition][0];
                });
            });
            cb(e, offsetFetchResponse);
        });
    });
};

HighLevelConsumer.prototype.offsetRequest = function (payloads, cb) {
    this.client.sendOffsetRequest(payloads, cb);
};


/**
 * Register a consumer against a group
 *
 * @param consumer to register
 *
 * @param {Client~failedToRegisterConsumerCallback} cb A function to call the consumer has been registered
 */
HighLevelConsumer.prototype.registerConsumer = function (cb) {
    this.client.zk.registerConsumer(this.options.groupId, this.id, this.payloads, function (err) {
        if (err) return cb(err);
        cb();
    })
    this.client.zk.listConsumers(this.options.groupId);
};

HighLevelConsumer.prototype.addTopics = function (topics, cb) {
    var self = this;
    if (!this.ready) {
        setTimeout(function () {
                self.addTopics(topics, cb)
            }
            , 100);
        return;
    }
    this.client.addTopics(
        topics,
        function (err, added) {
            if (err) return cb && cb(err, added);

            var payloads = self.buildPayloads(topics);
            // update offset of topics that will be added
            self.fetchOffset(payloads, function (err, offsets) {
                if (err) return cb(err);
                payloads.forEach(function (p) {
                    var offset = offsets[p.topic][p.partition];
                    if (!self.options.autoOffsetReset && offset === -1) {
                        offset = 0;
                    }
                    p.offset = offset;
                    self.topicPayloads.push(p);
                });
                // TODO: rebalance consumer
                cb && cb(null, added);
            });
        }
    );
};

HighLevelConsumer.prototype.removeTopics = function (topics, cb) {
    topics = typeof topics === 'string' ? [topics] : topics;
    this.payloads = this.payloads.filter(function (p) {
        return !~topics.indexOf(p.topic);
    });

    this.client.removeTopicMetadata(topics, cb);
};

HighLevelConsumer.prototype.close = function (force, cb) {
    this.ready = false;
    if(this.checkPartitionOwnershipInterval) {
        clearInterval(this.checkPartitionOwnershipInterval);
    }
    if(this.forcedRebalanceInterval) {
        clearInterval(this.forcedRebalanceInterval);
    }

    if (typeof force === 'function') {
        cb = force;
        force = false;
    }

    if (force) {
        this.commit(force, function (err) {
            this.client.close(cb);
        }.bind(this));
    } else {
        this.client.close(cb);
    }
};

HighLevelConsumer.prototype.stop = function (cb) {
    if (!this.options.autoCommit) return cb && cb();
    this.commit(true, function (err) {
        cb && cb();
    }.bind(this));
};

HighLevelConsumer.prototype.setOffset = function (topic, partition, offset) {
    this.topicPayloads.every(function (p) {
        if (p.topic === topic && p.partition == partition) {
            p.offset = offset;
            return false;
        }
        return true;
    });
};

HighLevelConsumer.prototype.pause = function () {
    this.paused = true;
};

HighLevelConsumer.prototype.resume = function () {
    this.paused = false;
    this.fetch();
};

module.exports = HighLevelConsumer;
