const jwt = require("jsonwebtoken");
const User = require("../models/Usres");
const secretKey = "Jc7xSToNXLdBXsmrR6Rej4EstJ6oMNbi";

function requireAuth(req, res, next) {
    const token = req.cookies.jwt;

    if (token) {
        jwt.verify(token, secretKey, (error, decodedToken) => {
            if (error) {
                res.redirect("/login");
            } else {
                next();
            }
        });
    } else {
        res.redirect("/login");
    }
}

function checkUser(req, res, next) {
    const token = req.cookies.jwt;
    res.locals.ueser = null;
    if (token) {
        jwt.verify(token, secretKey, async (error, decodedToken) => {
            if (error) {
                next();
            } else {
                let user = await User.findById(decodedToken.id);
                res.locals.user = user;
                next();
            }
        });
    } else {
        res.locals.user = null;
        next();
    }
}

module.exports = { requireAuth, checkUser };