var Datastore = require('nedb');
module.exports = new Datastore({filename: '../scenes.db', autoload: true});