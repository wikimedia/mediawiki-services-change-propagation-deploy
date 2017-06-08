var should = require('should');
var Shavaluator = require('..');
var testHelper = require('./test_helper');
var redisClient = null;
var shavaluator = new Shavaluator();
var sampleLuaCommands = require('./sample_lua_commands');
var assert = require('assert');

var describe = global.describe;
var it = global.it;
var before = global.before;
var beforeEach = global.beforeEach;

for (var commandName in sampleLuaCommands) {
  shavaluator.add(commandName, sampleLuaCommands[commandName]);
}

function prepopulateExampleSet(callback) {
  redisClient.zadd('testSet', 1, 'one', 2, 'two', 3, 'three', 4, 'four', function(err, result) {
    callback();
  });
}

var testKeys = {
  a: 1,
  b: 2,
  c: 3,
  d: 4
};

describe('Redis Lua commands', function() {
  before(function() {
    redisClient = testHelper.getRedisClient();
    shavaluator.redisClient = redisClient;
  });
  beforeEach(function(done) {
    redisClient.flushdb(function() {
      done();
    });
  });
  describe('setnx_pexpire', function() {
    var ttl = 50;
    describe("with key that hasn't been set yet", function() {
      it('returns 1 for keys the do not yet exist', function(done) {
        shavaluator.exec('setnx_pexpire', ['testKey'], ['testValue', ttl], function(err, result) {
          result.should.eql(1);
          done();
        });
      });
      it('sets the expiration correctly', function(done) {
        shavaluator.exec('setnx_pexpire', ['testKey'], ['testValue', ttl], function(err, result) {
          redisClient.pttl('testKey', function(err, result) {
            result.should.not.be.below(0);
            result.should.not.be.above(this.ttl);
            done();
          });
        });
      });
    });
    describe("with key that already exists", function(done) {
      beforeEach(function(done) {
        redisClient.set('testKey', 'testValue', function(err, result) {
          done();
        });
      });
      it('does not set the key', function(done) {
        shavaluator.exec('setnx_pexpire', ['testKey'], ['newValue', ttl], function(err, result) {
          result.should.eql(0);
          done();
        });
      });
      it('does not set an expiration time', function(done) {
        redisClient.pttl('testKey', function(err, result) {
          result.should.eql(-1);
          done();
        });
      });
    });
  });
  describe('zmembers', function() {
    describe('with nonexisting key', function() {
      it('returns an empty array', function(done) {
        shavaluator.exec('zmembers', ['nonexistingKey'], [], function(err, result) {
          assert.ok(!err, err && err.stack);
          result.length.should.eql(0);
          done();
        });
      });
    });
    describe('with prepopulated set', function() {
      beforeEach(function(done) {
        prepopulateExampleSet(done);
      });
      it('returns arguments that are members of the sorted set', function(done) {
        shavaluator.exec('zmembers', ['testSet'], ['one', 'three', 'five'], function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          result.should.eql(['one', 'three']);
          done();
        });
      });
    });
  });
  describe('znotmembers', function() {
    describe('with nonexisting key', function() {
      it('returns complete array', function(done) {
        var args = ['one', 'two', 'three'];
        shavaluator.exec('znotmembers', ['nonexistingKey'], args, function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          result.should.eql(args);
          done();
        });
      });
    });
    describe('with prepopulated set', function() {
      beforeEach(function(done) {
        prepopulateExampleSet(done);
      });
      it('returns arguments that are members of the sorted set', function(done) {
        shavaluator.exec('znotmembers', ['testSet'], ['zero', 'one', 'three', 'five'], function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          result.should.eql(['zero', 'five']);
          done();
        });
      });
    });
  });
  describe('delequal', function() {
    beforeEach(function(done) {
      var args = [];
      for (var k in testKeys) {
        var v = testKeys[k];
        args.push(k);
        args.push(v);
      }
      args.push(function(err, result) {
        done();
      });
      redisClient.mset.apply(redisClient, args);
    });
    it('returns zero if the key does not exist', function(done) {
      shavaluator.exec('delequal', ['nonexistent'], ['1'], function(err, result) {
        if (err != null) {
          err.should.be["false"]();
        }
        result.should.eql(0);
        done();
      });
    });
    it('deletes single keys when the matching value is sent', function(done) {
      shavaluator.exec('delequal', ['a'], [testKeys.a], function(err, result) {
        if (err != null) {
          err.should.be["false"]();
        }
        result.should.eql(1);
        redisClient.get('a', function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          (result === null).should.eql(true);
          done();
        });
      });
    });
    it('does not delete a single key when an unmatching value is sent', function(done) {
      shavaluator.exec('delequal', ['a'], ['x'], function(err, result) {
        if (err != null) {
          err.should.be["false"]();
        }
        result.should.eql(0);
        redisClient.get('a', function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          result.should.eql('1');
          done();
        });
      });
    });
    it('only deletes keys that match', function(done) {
      var deleteParams = {
        keys: ['a', 'b', 'c', 'd'],
        args: [1, 'x', 3, 'x']
      };
      shavaluator.exec('delequal', deleteParams.keys, deleteParams.args, function(err, result) {
        if (err != null) {
          err.should.be["false"]();
        }
        result.should.eql(2);
        redisClient.mget('a', 'b', 'c', 'd', function(err, result) {
          if (err != null) {
            err.should.be["false"]();
          }
          result.should.eql([null, '2', null, '4']);
          done();
        });
      });
    });
  });
});
