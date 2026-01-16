const readJob = require('./helpers/readCronJob')

exports.readCronJob = async (req, res) => {
    readCronJob(req, res)
}