const hackathon = require('../../loaders/hackathon')
// const student = require('./student')

module.exports = (args) => {
  return hackathon.load(args.id)
}