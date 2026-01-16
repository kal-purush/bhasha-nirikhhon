var scuttleup = require('scuttleup');
var crypto = require('crypto');
var events = require('events');
var sublevel = require('level-sublevel');

var cas = function(db, opts) {
  var subs = sublevel(db)
  var log = scuttleup(subs.sublevel('log'), {id: opts.id});
  var store = subs.sublevel('store');
  var that = new events.EventEmitter();

  var onchange = function(dta) {
    var onbatch = function(err) {
      if (err) that.emit('error', err);
    };

    var onget = function(err, heads) {
      if (err && err.message !== 'Key not found in database') return that.emit('error', err);
      heads = heads? JSON.parse(heads) : {};
      heads[dta.peer] = {peer: dta.peer, seq: dta.seq};
      store.batch([
        JSON.parse(dta.entry.toString()),
        {type: 'put', key: 'heads', value: JSON.stringify(heads)}
      ], onbatch);
    };

    store.get('heads', onget);
  };

  store.get('heads', function(err, heads) {
    if (err && err.message !== 'Key not found in database') return that.emit('error', err);
    if (!heads) return log.createReadStream({live: true}).on('data', onchange);
    heads = JSON.parse(heads);
    var hds = Object.keys(heads).map(function(key) {
      return heads[key];
    });
    log.createReadStream({live: true, since: hds}).on('data', onchange);
  });

  that.syncStream = function() {
    return log.createReplicationStream();
  };

  that.put = function(content, cb) {
    cb = cb || function() {};
    var key = crypto.createHash('sha256').update(content).digest('base64');
    log.append(JSON.stringify({type:'put', key: key, value: content}), function(err) {
      if (err) return cb(err);
      cb(null, key);
    });
  };

  that.del = function(key) {
    log.append(JSON.stringify({type:'del', key: key}));
  };

  that.get = store.get;
  that.createValueStream = store.createValueStream;
  that.createKeyStream = store.createKeyStream;
  that.createReadStream = store.createReadStream;

  return that;
};

module.exports = cas;