'use strict';

var zookeeper = require('node-zookeeper-client'),
    util = require('util'),
    async = require("async"),
    EventEmiter = require('events').EventEmitter,
    debug = require('debug')('kafka-node:zookeeper');

/**
 * Provides kafka specific helpers for talking with zookeeper
 *
 * @param {String} [connectionString='localhost:2181/kafka0.8'] A list of host:port for each zookeeper node and
 *      optionally a chroot path
 *
 * @constructor
 */
var Zookeeper = function (connectionString, options) {
    this.client = zookeeper.createClient(connectionString, options);

    var that = this;
    this.client.on('connected', function () {
        that.listBrokers();
    });
    this.client.connect();
};

util.inherits(Zookeeper, EventEmiter);

Zookeeper.prototype._createFullPath = function(fullPath, skipLast, cb) {
    var self = this;
    var asyncSeg = [];
    var pathSeg = fullPath.split('/');
    var idx;
    if (typeof skipLast === 'function') {
        cb = skipLast;
        skipLast = false;
    }
    // skip the leading and trailing empty paths
    if (pathSeg[0] === '') { pathSeg.shift(); }
    if (pathSeg[pathSeg.length - 1] === '') { pathSeg.pop(); }
    // skip the last path segment creation if so specified
    if (skipLast) { pathSeg.pop(); }
    if (!pathSeg.length) {
        // no path segments left, nothing to do
        return cb();
    }
    // assemble the functions iteratively creating
    // the full path
    asyncSeg.push(function(callback) {
        callback(null, 1, pathSeg);
    });
    for (idx = 1; idx <= pathSeg.length; idx++) {
        asyncSeg.push(function(index, segments, callback) {
            var path = '/' + segments.slice(0, index).join('/');
            self.client.create(path, function (err, p) {
                // simply carry on
                callback(null, index + 1, segments);
            });
        });
    }
    // do it
    async.waterfall(asyncSeg, function(err) {
        if (err) {
            return cb(err);
        }
        cb();
    });
};

Zookeeper.prototype.registerConsumer = function (groupId, consumerId, payloads, cb) {
    var path = '/consumers/' + groupId + '/ids/' + consumerId;
    var self = this;

    async.series([
        function (callback) {
            self._createFullPath(path, true, callback);
        },
        function (callback) {
            self.client.create(
                path,
                null,
                null,
                zookeeper.CreateMode.EPHEMERAL,
                function (error, p) {
                    if (error) {
                        return callback(error);
                    }
                    callback();
                });
        },
        function (callback) {
            var metadata = '{"version":1,"subscription":'
            metadata += '{'
            var sep = '';
            payloads.map(function (p) {
                metadata += sep + '"' + p.topic + '": 1'
                sep = ', ';
            });
            metadata += '}'
            var milliseconds = (new Date).getTime();
            metadata += ',"pattern":"white_list","timestamp":"' + milliseconds + '"}';
            self.client.setData(path, new Buffer(metadata), function (error, stat) {
                if (error) {
                    return callback(error);
                }
                debug('Node: %s was created.', path + '/ids/' + consumerId);
                cb();
            });
        }
    ], function (err) {
        if (err) {
            return cb(err);
        }
        cb();
    });
};

Zookeeper.prototype.getConsumersPerTopic = function (groupId, cb) {
    var consumersPath = '/consumers/' + groupId + '/ids';
    var brokerTopicsPath = '/brokers/topics';
    var consumers = [];
    var path = '/consumers/' + groupId;
    var that = this;
    var consumerPerTopicMap = new ZookeeperConsumerMappings();

    async.series([
            function (callback) {
                that.client.getChildren(consumersPath, function (error, children, stats) {
                    if (error) {
                        callback(error);
                        return;
                    }
                    else {
                        debug('Children are: %j.', children);
                        consumers = children;
                        async.each(children, function (consumer, cbb) {
                            var path = consumersPath + '/' + consumer;
                            that.client.getData(
                                path,
                                function (error, data) {
                                    if (error) {
                                        cbb(error);
                                    }
                                    else {
                                        try {
                                            var obj = data ? JSON.parse(data.toString()) : {};
                                            // For each topic
                                            for (var topic in obj.subscription) {
                                                if (consumerPerTopicMap.topicConsumerMap[topic] == undefined) {
                                                    consumerPerTopicMap.topicConsumerMap[topic] = [];
                                                }
                                                consumerPerTopicMap.topicConsumerMap[topic].push(consumer);

                                                if (consumerPerTopicMap.consumerTopicMap[consumer] == undefined) {
                                                    consumerPerTopicMap.consumerTopicMap[consumer] = [];
                                                }
                                                consumerPerTopicMap.consumerTopicMap[consumer].push(topic);
                                            }

                                            cbb();
                                        } catch (e) {
                                            debug(e);
                                            callback(new Error("Unable to assemble data"));
                                        }
                                    }
                                }
                            );
                        }, function (err) {
                            if (err)
                                callback(err);
                            else
                                callback();
                        });
                    }
                });
            },
            function (callback) {
                Object.keys(consumerPerTopicMap.topicConsumerMap).forEach(function (key) {
                    consumerPerTopicMap.topicConsumerMap[key] = consumerPerTopicMap.topicConsumerMap[key].sort();
                });
                callback();
            },
            function (callback) {
                async.each(Object.keys(consumerPerTopicMap.topicConsumerMap), function (topic, cbb) {
                    var path = brokerTopicsPath + '/' + topic;
                    that.client.getData(
                        path,
                        function (error, data) {
                            if (error) {
                                cbb(error);
                            }
                            else {
                                var obj = JSON.parse(data.toString());
                                // Get the topic partitions
                                var partitions = Object.keys(obj.partitions).map(function (partition) {
                                    return partition;
                                });
                                consumerPerTopicMap.topicPartitionMap[topic] = partitions.sort(compareNumbers);
                                cbb();
                            }
                        }
                    );
                }, function (err) {
                    if (err)
                        callback(err);
                    else
                        callback();
                });
            }],
        function (err) {
            if (err) {
                debug(err);
                cb(err);
            }
            else cb(null, consumerPerTopicMap);
        });
};

function compareNumbers(a, b) {
    return a - b;
}

Zookeeper.prototype.listBrokers = function (cb) {
    var that = this;
    var path = '/brokers/ids';
    this.client.getChildren(
        path,
        function () {
            that.listBrokers();
        },
        function (error, children) {
            if (error) {
                debug('Failed to list children of node: %s due to: %s.', path, error);
                that.emit('error', error);
                return;
            }

            if (children.length) {
                var brokers = {};
                async.each(children, getBrokerDetail, function (err) {
                    if (err) {
                        that.emit('error', err);
                        return;
                    }
                    if (!that.inited) {
                        that.emit('init', brokers);
                        that.inited = true;
                    } else {
                        that.emit('brokersChanged', brokers)
                    }
                    cb && cb(brokers); //For test
                });
            } else {
                if (that.inited)
                    return that.emit('brokersChanged', {})
                that.inited = true;
                that.emit('init', {});
            }

            function getBrokerDetail (id, cb) {
                var path = '/brokers/ids/' + id;
                that.client.getData(path,function (err, data) {
                    if (err) return cb(err);
                    brokers[id] = JSON.parse(data.toString());
                    cb();
                });
            }
        }
    );
};


Zookeeper.prototype.listConsumers = function (groupId) {
    var that = this;
    var path = '/consumers/' + groupId + '/ids';
    this.client.getChildren(
        path,
        function () {
            if (!that.closed) {
                that.listConsumers(groupId);
            }
        },
        function (error, children) {
            if (error) {
                debug(error);
                // Ignore NO_NODE error here #157
                if (error.name !== 'NO_NODE') {
                    that.emit('error', error);
                }
            } else {
                that.emit('consumersChanged');
            }
        }
    );
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            debug('Got event: %s.', event);
            if (watch)
                self.topicExists(topic, cb);
        },
        function (error, stat) {
            if (error) return cb(error);
            cb(null, !!stat, topic);
        }
    );
};

Zookeeper.prototype.deletePartitionOwnership = function (groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;
    this.client.remove(
        path,
        function (error) {
            if (error)
                cb(error);
            else {
                debug("Removed partition ownership %s", path);
                cb();
            }
        }
    );
};

Zookeeper.prototype.addPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition;
    var self = this;
    var isLocked = false;

    async.series([
        function (callback) {
            self.lockPartition(groupId, topic, partition, consumerId, function(err, locked) {
                if (err) {
                    return callback(err);
                }
                if (!locked) {
                    return callback('Partition locked by another consumer!');
                }
                isLocked = true;
                callback();
            });
        },
        function (callback) {
            self._createFullPath(path, true, callback);
        },
        function (callback) {
            self.client.create(
                path,
                new Buffer(consumerId),
                null,
                zookeeper.CreateMode.EPHEMERAL,
                function (error, retPath) {
                    if (!error) {
                        return callback();
                    }
                    if (/node_exists/i.test(error)) {
                        return self.client.remove(path, function(err) {
                            if (err) {
                                return callback(err);
                            }
                            self.client.create(
                                path,
                                new Buffer(consumerId),
                                null,
                                zookeeper.CreateMode.EPHEMERAL,
                                callback
                            );
                        });
                    }
                    callback(error);
                });
        },
        function (callback) {
            self.client.exists(path, null, function (error, stat) {
                if (error) {
                    return callback(error);
                }
                if (stat) {
                    debug('Gained ownership of %s by %s.', path, consumerId);
                    return callback();
                }
                callback("Path wasn't created");
            });
        }
    ], function (err) {
        if (isLocked) {
            // whatever happened, we need to unlock the partition
            return self.unlockPartition(groupId, topic, partition, consumerId, function(partErr) {
                // we don't care about unlock errors as
                // there's nothing we can do here
                if (err) {
                    return cb(err);
                }
                cb();
            });
        }
        if (err) {
            return cb(err);
        }
        cb();
    });
};

Zookeeper.prototype.lockPartition = function(groupId, topic, partition, clientId, cb) {
    var self = this;
    var path = '/consumers/' + groupId + '/lock/' + topic + '/' + partition;
    async.series([
        function (callback) {
            self._createFullPath(path, true, callback);
        },
        function (callback) {
            self.client.create(
                path, new Buffer(clientId), null, zookeeper.CreateMode.EPHEMERAL,
                function(err, p) {
                    if (err) {
                        // if the node exists, another consumer has
                        // locked this topic/partition combo
                        if (/node_exists/i.test(err)) {
                            return callback('locked');
                        }
                        // something else is going on, report it
                        return callback(err);
                    }
                    // the node has been created, proceed
                    callback();
                }
            );
        }
    ], function (err) {
        if (!err) {
            return cb(null, true);
        }
        if (err === 'locked') {
            return cb(null, false);
        }
        cb(err);
    });
};

Zookeeper.prototype.unlockPartition = function (groupId, topic, partition, clientId, cb) {
    var self = this;
    var path = '/consumers/' + groupId + '/lock/' + topic + '/' + partition;
    async.series([
        // ensure this client has locked the partition
        function (callback) {
            self.client.getData(path, function (error, data) {
                if (error) {
                    return callback(error);
                }
                var ownerId = (data || '').toString();
                if (ownerId !== clientId) {
                    return callback('Partition locked by ' + ownerId);
                }
                callback();
            });
        },
        // now remove the lock
        function (callback) {
            self.client.remove(path, function (error) {
                if (error) {
                    return callback(error);
                }
                callback();
            });
        }
    ], function (err) {
        if (err) {
            return cb(err);
        }
        cb();
    });
};

Zookeeper.prototype.getPartitionOwnership = function(groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition;
    var self = this;
    async.waterfall([
        function (callback) {
            self.client.exists(path, null, function (error, stat) {
                if (error) {
                    return callback(error);
                }
                if (stat) {
                    return callback(null, true);
                }
                callback(null, false);
            });
        },
        function (nodeExists, callback) {
            if (!nodeExists) {
                return callback(null, '');
            }
            self.client.getData(
                path,
                function (error, data) {
                    if (error) {
                        return callback(error);
                    }
                    callback(null, (data || '').toString());
                }
            );
        }
    ], function (err, ownerId) {
        if (err) {
            return cb(err);
        }
        cb(null, ownerId);
    });
};

Zookeeper.prototype.checkPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
    this.getPartitionOwnership(groupId, topic, partition, function(err, ownerId) {
        if (err) {
            return cb(err);
        }
        if (consumerId !== ownerId) {
            return cb("Consumer not registered " + consumerId);
        }
        cb();
    });
};

Zookeeper.prototype.getFreePartitions = function(groupId, cb) {
    var self = this;
    self.getConsumersPerTopic(groupId, function(err, map) {
        if (err) {
            return cb(err);
        }
        var topicMap = map.topicPartitionMap;
        var list = [];
        Object.keys(topicMap).forEach(function(topic) {
            var noCons = map.topicConsumerMap[topic].length;
            topicMap[topic].forEach(function(partition) {
                list.push({ topic: topic, partition: partition, noConsumers: noCons });
            });
        });
        if (!list.length) {
            return cb();
        }
        async.mapSeries(list, function(obj, callback) {
            self.getPartitionOwnership(groupId, obj.topic, obj.partition, function(error, ownerId) {
                if (!ownerId && !error) {
                    return callback(null, obj);
                }
                callback();
            });
        }, function(error, freeList) {
            if (error) {
                return cb(error);
            }
            cb(null, freeList.filter(function(item) { return !!item; }));
        });
    });
};

Zookeeper.prototype.close = function () {
    this.closed = true;
    this.client.close();
};

var ZookeeperConsumerMappings = function () {
    this.consumerTopicMap = {};
    this.topicConsumerMap = {};
    this.topicPartitionMap = {};
};


exports.Zookeeper = Zookeeper;
exports.ZookeeperConsumerMappings = ZookeeperConsumerMappings;
