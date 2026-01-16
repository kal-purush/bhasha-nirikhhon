/**
 * Contains database related operations
 */
const config = require('config.json');
const mysql = require('mysql2/promise');
const { Sequelize } = require('sequelize');

module.exports = db = {};

initialize();

/**
 * Initalizes Database with required models
 */
async function initialize() {
    // create db if it doesn't already exist
    const { host, port, user, password, database } = config.database;
    const connection = await mysql.createConnection({ host, port, user, password });
    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${database}\`;`);

    // connect to db
    const sequelize = new Sequelize(database, user, password, { 
        dialect: 'mysql', 
        logging: false 
    });

    // init models and add them to the exported db object
    db.User = require('../users/user.model')(sequelize);
    db.Product = require('../products/product.model')(sequelize);

    // define associations
    db.User.hasMany(db.Product);
    db.Product.belongsTo(db.User);

    // sync all models with database
    await sequelize.sync();
}