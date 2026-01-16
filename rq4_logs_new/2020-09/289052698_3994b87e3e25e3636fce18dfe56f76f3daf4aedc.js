const production = require('./webpack.production.config')
const development = require('./webpack.development.config')
const common = require('./webpack.common.config')

module.exports = {
  production,
  development,
  common,
}