const { Pool } = require('pg');
const url = require('url');
require('env2')('./config.env');

if (!process.env.DATABASE_URL) throw new Error('Environment variable DATABASE_URL must be set');

// Create a params object from the URL (use config variables!)
const params = url.parse(process.env.DATABASE_URL);
const [username, password] = params.auth.split(':');
const dbName = params.pathname.split('/')[1];

// Options object with all the connection extracted parameters
const options = {
  host: params.hostname,
  port: params.port,
  database: params.pathname.split('/')[1],
  max: process.env.DB_MAX_CONNECTIONS || 2,
  user: username,
  password,
};

// SSL should be enabled if you're not testing locally
options.ssl = (options.host !== 'localhost');

// Export pool for use in other modules
module.exports = new Pool(options);