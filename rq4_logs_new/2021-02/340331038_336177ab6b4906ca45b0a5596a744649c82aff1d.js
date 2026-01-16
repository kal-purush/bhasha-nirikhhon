const { Pool } = require('pg');
const config = require('../config');

const pool = new Pool(config.db);

module.exports = {
  query: (text, params, callback) => {
    console.log('\x1b[36mExecuting query: \x1b[0m\n' + text);
    return pool.query(text, params, callback);
  },

  printErr: (err) => {
    console.log(
      '\x1b[31mError ' +
        err.code +
        ': ' +
        err.detail +
        (err.where ? ' @' + err.where : '' + '\x1b[33m')
    );
    console.log(err.stack + '\x1b[0m');
  },
};