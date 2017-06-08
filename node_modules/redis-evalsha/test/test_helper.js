var fs = require('fs');
var path = require('path');
var redis = require('redis');

var config = {};
try {
  config = require('./redis.json');
} catch (err) {
  config = require('./redis.sample.json');
}

process.env.NODE_ENV = "test";

module.exports = {
  getRedisClient: function() {
    return redis.createClient(config.port, config.host, config.options);
  }
};
