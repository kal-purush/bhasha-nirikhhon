var jwt = require('jsonwebtoken');
var config = require('../../config');
var bcrypt = require('bcrypt');
var DB = require ("./DatabaseConnection");

exports.checkToken = function(req, res, next) {
    console.log(req.url.indexOf("authen"));
    if(req.url.indexOf("authen") == -1) {
        var token = req.get("token");
        jwt.verify(token, config.secretKey, function(err, decoded) {
            if (err) {
                return res.status(401).send({
                    status: "unauthorized"
                });
            }
            req.userObj = decoded;
            next();
        });
    }
    else {
        next();
    }
}

// username, password
exports.authentication = function(req, res) {
    const query = req.query;
    var username = query.username;
    var password = query.password;
    var dbQuery = "SELECT * from User where username = '" + username + "'";
    DB.getDB().query(dbQuery, function(err, result, field) {
        if (err) {
            res.status(400).json({'status' : 'not found user'});
        }
        else {
            if(result[0].username != null) {
                bcrypt.compare(password, result[0].password, function (err, isEqual) {
                    if (err) {
                        throw err;
                    }
                    if (isEqual) {
                        var tokenObj = {
                            "userId": result[0].userId,
                            "username": result[0].username,
                            "name": result[0].name
                        };
                        var token = jwt.sign(tokenObj, config.secretKey, {expiresIn: 86400});
                        res.send({"status": 'success', 'token': token});
                    }
                    else {
                        res.status(400).send({'status': 'wrong password'});
                    }
                });
            }
            else {
                res.status(400).json({'status' : 'not found user'});
            }
        }
    })
}