var getArgvCfg = require('./get-argv-cfg.js'),
    getEnvCfg = require('./get-env-cfg.js'),
    syncOrAsync = require('./sync-or-async.js');

function configResolver(name, opts, defaults, overrides, ready){
  overrides = overrides || {};

  var argvObj = {};

  // handle argv parsing / Identify config coming from argv early
  if (opts.useArgv) {
    argvObj = getArgvCfg(process.argv, opts.structure)
  }

  var environment,
      envVarPrefix = name.toUpperCase().replace(/[-\W]/g, '_');

  environment = argvObj.env ||
    argvObj.environment ||
    overrides.env ||
    overrides.environment ||
    process.env[ envVarPrefix + '_ENV' ] ||
    process.env.ENV ||
    process.env.ENVIRONMENT ||
    process.env.NODE_ENV;
  
  var hasCallback = ('function' === typeof ready);

  var lib = syncOrAsync({
    async: (opts.async || hasCallback),
    timeout: opts.timeout
  });

  // Identify config from process.env
  var envObj = (!opts.useEnv) ? {} : getEnvCfg(envVarPrefix, opts.structure);

  // Identify config from package.json
  var pkgCfgs = (!opts.usePkg) ? [] : lib.getPkgCfg({
    name: name,
    dirs: opts.pkgSearchDirs
  });

  // Identify which config files actually exist
  var found = lib.getPaths(name, opts.patterns, environment);

  // read and parse each config file
  var fileCfgs = lib.processFiles(found);

  // merge the configs found in each source
  var config = lib.mergeConfigs(
    name,
    environment,
    defaults,
    overrides,
    pkgCfgs,
    envObj,
    argvObj,
    fileCfgs
  );

  const retVal = lib.done(config);
  if (hasCallback) {
    retVal.then(
      function complete(cfg) {
        return setImmediate(function() { ready(null, cfg); });
      },
      function fail(e) {
        return setImmediate(function() { ready(e); });
      }
    );
    return;
  }
  return retVal;
};

module.exports = configResolver;