var spawn     = require('child_process').spawn,
    os_name   = process.platform.replace('win32', 'windows').replace('darwin', 'mac'),
    os_module = require(require('path').join(__dirname, os_name)),
    Emitter   = require('events').EventEmitter;

var child,
    stopped,
    watchers  = {},
    respawned = {};

var respawn = (trigger, cb) => {
  if (respawned[trigger]) return cb(new Error("Unable to start watcher command."));

  setTimeout(() => {
    if (stopped) return;
    console.log('Respawning!', trigger);
    respawned[trigger] = true;
    watch(trigger, cb);

  }, 100)
}

var watch = (trigger, cb) => {
  var watcher,
      error,
      exit_error,
      stopped = false;

  if (!trigger)
    cb(new Error('Trigger name required'));

  // Create child process if doesn't exist
  if (!child) child = spawn(os_module.command[0], os_module.command.slice(1));

  var done = (exit) => {
    if (stopped) {
      watchers = {};
      return;
    }

    if (exit) {  //Something went wrong
      emitter = null;
      if (!respawned[trigger])
        return respawn(trigger, cb);
    } else {
      return cb && cb(error, !error && emitter)
    }
  }

  child.on('error', (err) => {
    if (err.code == 'ENOENT')
      err.message = 'ENOENT - Command not found: ' + os_module.command;
    error = err;
  })

  child.on('exit', (code) => {
    exit_error = true;
    child = null;
    delete watchers[trigger];
    done(true);
  })

  if (!watchers[trigger]) {
    var emitter = new Emitter();
    watcher = new os_module.Watcher(child, trigger, emitter);
    watchers[trigger] = watcher;
  }

  setTimeout(() => {
    if (!exit_error) {
      done();
    }
  }, 100)

}

var unwatch = (trigger) => {
  // if there are no watchers, return
  if (Object.keys(watchers).length == 0) return;

  // find watcher and stop it
  var watcher = watchers[trigger];

  if (!watcher) return;

  watcher.stop();
  delete watchers[trigger];

  // console.log('Unwatched ' + trigger + '. Current: ' + Object.keys(watchers).length);

  // if no watchers are left, stop process
  if (child && Object.keys(watchers).length == 0) {
    // console.log('No more watchers, so killing child.');
    stopped = true;
    child.kill();
  }

}

module.exports.watch = watch;
module.exports.unwatch = unwatch;

module.exports.stop = () => {
  respawned = {};
  for (var trigger in watchers)
    module.exports.unwatch(trigger);
}