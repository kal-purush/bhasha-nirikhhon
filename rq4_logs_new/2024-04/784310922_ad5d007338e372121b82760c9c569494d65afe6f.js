const items = require('../assets/data')

exports.handler = async () => {
  return {
    statusCode: 200,
    body: JSON.stringify(items),
  }
}