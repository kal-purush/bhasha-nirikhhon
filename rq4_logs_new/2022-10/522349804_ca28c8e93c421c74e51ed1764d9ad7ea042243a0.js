const sequelize = require('sequelize');
const db = require('../configs/database.js');

const ArchiveItem = db.define('archiveItem',{
    id: {
        primaryKey: true,
        type: sequelize.INTEGER(10),
        autoIncrement: true,
    },
    item_id: {
        type: sequelize.STRING(30),
        allowNull: false,
        unique: true,
    },
    fb_id: {
        type: sequelize.STRING(30),
        allowNull: false
    },
    fb_user_name: {
        type: sequelize.STRING(100),
        allowNull: false
    },
    fb_message_id: {
        type: sequelize.STRING(30),
        allowNull: true
    },
    fb_post_id: {
        type: sequelize.STRING(30),
        allowNull: true
    },
    has_unread_message:{
        type: sequelize.BOOLEAN,
        defaultValue: false,
    },
    has_unsent_message:{
        type: sequelize.BOOLEAN,
        defaultValue: false,
    },
    last_auto_step:{
        type: sequelize.STRING(30),
        allowNull: true
    },
    last_auto_timeStamp:{
        type: sequelize.STRING(15),
        allowNull: true
    },
    last_owner_name: {
        type: sequelize.STRING(100),
        allowNull: true
    },
    last_owner_timestamp: {
        type: sequelize.STRING(15),
        allowNull: true
    }
},{
    timestamps: false,
    // freezeTableName: true,
    // tableName: 'archiveItems'
});
// ArchiveItem.sync({force: true});
module.exports = ArchiveItem;