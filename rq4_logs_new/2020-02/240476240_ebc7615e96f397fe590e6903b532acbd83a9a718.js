const db = require("../db");
const Sequelize = require("sequelize");
const Message = require("../message/model");

const Channel = db.define("channel", {
  name: Sequelize.STRING
});

Message.belongsTo(Channel);
Channel.hasMany(Message);

module.exports = Channel;