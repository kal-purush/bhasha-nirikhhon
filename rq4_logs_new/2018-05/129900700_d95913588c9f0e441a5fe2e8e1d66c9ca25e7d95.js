const Sequelize = require('sequelize');
module.exports = new Sequelize('kaerimasu', 'kaerimasu', 'takosu111', {
  host: 'localhost',
  dialect: 'sqlite',
  operatorsAliases: false,

  pool: {
    max: 5,
    min: 0,
    acquire: 30000,
    idle: 10000
  },

  // SQLite only
  storage: '.db/db.sqlite'
});