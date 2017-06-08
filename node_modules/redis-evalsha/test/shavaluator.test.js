var async = require('async');
var should = require('should');
var Shavaluator = require('..');
var testHelper = require('./test_helper');
var redisClient = null;
var assert = require('assert');

var describe = global.describe;
var it = global.it;
var before = global.before;
var beforeEach = global.beforeEach;

describe('Shavaluator', function() {
  before(function() {
    redisClient = testHelper.getRedisClient();
  });
  beforeEach(function(done) {
    this.shavaluator = new Shavaluator(redisClient);
    async.parallel([
      function(cb) {
        redisClient.flushdb(cb);
      }, function(cb) {
        redisClient.send_command('script', ['flush'], cb);
      }
    ], done);
  });
  it('should pass an error if the script was never added', function(done) {
    this.shavaluator.exec('nonexistent', [], [], function(err, result) {
      err.message.should.match(/unrecognized script/);
      done();
    });
  });
  describe('eval()', function() {
    beforeEach(function(done) {
      this.shavaluator.add('echo', 'return ARGV[1]');
      this.shavaluator.add('luaget', "return redis.call('GET', KEYS[1])");
      this.shavaluator.add('setnxget', "redis.call('SETNX', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1]);");
      done();
    });
    it('evaluates scripts with arguments', function(done) {
      this.shavaluator.exec('echo', [], ['testValue'], function(err, result) {
        assert.ok(!err, err && err.stack);
        result.should.eql('testValue');
        done();
      });
    });
    it('runs the same script multiple times', function(done) {
      var shavaluator = this.shavaluator;
      var samples = [];

      for (var i = 0; i <= 10; i += 1) {
        pushOne(i);
      }
      async.waterfall(samples, done);

      function pushOne(i) {
        samples.push(function(cb) {
          shavaluator.exec('echo', [], ["test" + i], function(err, result) {
            assert.ok(!err, err && err.stack);
            assert.strictEqual(result, "test" + i);
            cb();
          });
        });
      }
    });
    it('evaluates scripts with keys', function(done) {
      redisClient.set('testKey', 'testValue');
      this.shavaluator.exec('luaget', ['testKey'], [], function(err, result) {
        assert.ok(!err, err && err.stack);
        result.should.eql('testValue');
        done();
      });
    });
    it('evaluates scripts with both keys and arguments', function(done) {
      var t = Date.now().toString();
      this.shavaluator.exec('setnxget', ['hey'], [t], function(err, result) {
        assert.ok(!err, err && err.stack);
        result.should.eql(t);
        done();
      });
    });
  });
});

