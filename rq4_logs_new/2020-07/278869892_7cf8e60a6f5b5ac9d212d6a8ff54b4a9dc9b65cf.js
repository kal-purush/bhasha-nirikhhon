module.exports = function addDate(req, res, next){
    req.body.date = Date.now()
    return next();
}