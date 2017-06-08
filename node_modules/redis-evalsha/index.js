var crypto = require('crypto');

module.exports = Shavaluator;

function Shavaluator(redisClient) {
  this.redisClient = redisClient;
  this.scripts = {};
}

Shavaluator.prototype.add = function(name, body) {
  this.scripts[name] = {
    body: body,
    hash: sha1(body),
  };
};

Shavaluator.prototype.execWithClient = function(redisClient, name, keys, args, callback) {
  var script = this.scripts[name];
  if (!script) return process.nextTick(errorScriptNotFound);

  var params = [script.hash, keys.length].concat(keys).concat(args);
  redisClient.evalsha(params, function(err, res) {
    if (err) {
      if (/NOSCRIPT/.test(err.message)) {
        var params = [script.body, keys.length].concat(keys).concat(args);
        redisClient.send_command('eval', params, callback);
      } else {
        callback(err, res);
      }
    } else {
      callback(null, res);
    }
  });

  function errorScriptNotFound() {
    callback(new Error("unrecognized script name: " + name));
  }
}

Shavaluator.prototype.exec = function(name, keys, args, callback) {
  return this.execWithClient(this.redisClient, name, keys, args, callback);
};

function sha1(string) {
  return crypto.createHash('sha1').update(string, 'utf8').digest('hex');
}
