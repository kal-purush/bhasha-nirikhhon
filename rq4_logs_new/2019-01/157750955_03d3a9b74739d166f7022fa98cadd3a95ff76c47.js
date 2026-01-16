const fs = require('fs');
const Redis = require('ioredis');
const util = require('util');

module.exports = {
  createClient: function(app) {
    try {
      app.status=app.status||{};
      app.status.redis={};

      let REDIS_PASSWORD = process.env.REDIS_PASSWORD
      if (fs.existsSync('/etc/redis/redis-password')) {
        REDIS_PASSWORD = fs.readFileSync('/etc/redis/redis-password').toString(); 
      }
  
      const namespace = process.env.NAMESPACE.replace('-','_').toUpperCase();
      /*
      const nodes = [];
      nodes.push({ 
        host: process.env[namespace + '_FECACHE_MASTER_SERVICE_HOST'], 
        port: Number.parseInt(process.env[namespace +'_FECACHE_MASTER_SERVICE_PORT'])
      });
      nodes.push({ 
        host: process.env[namespace + '_FECACHE_SLAVE_SERVICE_HOST'], 
        port: Number.parseInt(process.env[namespace +'_FECACHE_SLAVE_SERVICE_PORT'])
      });
      
      app.cache = new Redis.Cluster(nodes, {
        scaleReads: 'all', // all, slave,
        slotsRefreshTimeout: 5000,
        slotsRefreshInterval: 10000,
        redisOptions: {
          password: REDIS_PASSWORD
        }
      });
      */

     app.cache = new Redis({
      host: process.env[namespace + '_FECACHE_MASTER_SERVICE_HOST'], 
      port: Number.parseInt(process.env[namespace +'_FECACHE_MASTER_SERVICE_PORT']),
      password: REDIS_PASSWORD
     })
  
      app.cache.on('connect', function(err){
        app.status.redis.connectionStatus = 'connected';
        app.status.redis.serverInfo = Object.assign({}, app.cache.server_info);
      });
      app.cache.on('error', function(err){
        app.status.redis.lastError=err;
        console.error('REDIS ERROR: ',err);
      });
      app.cache.on('ready', function(err){
        app.status.redis.connectionStatus = 'ready';
      });
      app.cache.on('end', function(err){
        app.status.redis.connectionStatus = 'disconnected';
      });
      app.cache.on('reconnecting', function(err){
        app.status.redis.connectionStatus = 'reconnecting';
      });
    } catch(e) {
      console.error('Unable to read the REDIS credentials file.');
      console.error( util.inspect(e, {color:true}));
    }
  }
}