export {};
const Sequelize = require('sequelize');
const _config = require("../config/keys.ts")

const sequelize = new Sequelize(_config.database, _config.user, _config.pass, {
    dialect: 'mariadb',
    logging: false
})

const Group = sequelize.define('group', {
    Name: {type: Sequelize.STRING, allowNull: false}
    ,ContactInfo: {type: Sequelize.STRING, allowNull: false}
})




module.exports = Group