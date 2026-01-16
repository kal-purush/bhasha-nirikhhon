require('dotenv').config()

var config = {}

config.yaleclub = {}
config.yaleclub.username = process.env.YALECLUB_USERNAME
config.yaleclub.password = process.env.YALECLUB_PASSWORD

module.exports = config