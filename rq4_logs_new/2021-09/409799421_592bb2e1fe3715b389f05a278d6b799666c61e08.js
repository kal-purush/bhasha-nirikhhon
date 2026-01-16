const custosConnector = require('../services/custos-connector')


module.exports = function authenticate(req, res, next) {
    if (req.user) {
        next();
    } else {
        const authToken = req.header("authorization")
        if (authToken) {
            const splitted = authToken.split('Bearer ')
            custosConnector.isAuthenticated(splitted[1]).then((res) => {
                if (res) {
                    next()
                }else {
                    res.status(401).send("Unauthorized");
                }
            }).catch(ex => {
                res.status(401).send("Unauthorized");
            });
        } else {
            res.status(401).send("Unauthorized");
        }

    }
};