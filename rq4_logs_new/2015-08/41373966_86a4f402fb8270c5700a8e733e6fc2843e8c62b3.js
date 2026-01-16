var clean = require('./empty')
var admins = require('./admin')
var questions = require('./question')

clean.seed()
  .then(function () {
    return Promise
      .all([admins.seed(), questions.seed()])
      .then(function () { process.exit() })
      .catch(function (err) {
        throw err
      })
  })