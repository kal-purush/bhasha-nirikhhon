var USER = require("../models/users-table");
var config = require("../../port-config");

var secretKey = config.secretKey;

var jsonwebtoken = require('jsonwebtoken');

function createToken(user){
    var token = jsonwebtoken.sign({
        _id: user._id,
        name: user.name,
        email: user.email
    }, secretKey);
    return token;
};
module.exports = function(app, express){
    var api = express.Router();

    // save data in mongoose for new users.
    api.post('/signup', function(req, res){
        var user = new USER({
            name: req.body.userName,
            email: req.body.userEmail,
            password: req.body.userPassword,
            createdDate: new Date()
        });                
        user.save(function(err){
            if(err){
                console.log(err)
                res.send(err);
                return;
            }
            res.json({message: 'User has been created!'});
        });
    });

    // get all users data
    api.get('/all-users', function(err, res){
        USER.find({}, function(err, users){
            if(err){
                res.send(err);
                return;
            }
            res.json(users);
        });
    });

    // check auth user
    api.post('/login', function(req, res){
        
        USER.findOne({
            email: req.body.email
        }).select('password').exec(function(err, user){
            if(err) throw err;
            
            if(!user){
                res.send({ message: "User does not exist."});
            }else if(user){
                var passwordValidation = user.comparePassword(req.body.password);
                if(!passwordValidation){
                    res.json({ 
                        statusText: "error",
                        status: false,
                        message: "Invalid Password." });
                }else{
                    // get token
                    var token = createToken(user);
                    res.json({
                        statusText: "OK",
                        status: true,
                        message: "login successfuly.",
                        token: token
                    });
                }
            }
        });
    });
    // update password
    api.post('/forget-password', function(req, res){
        USER.findOne({
            email: req.body.email
        }).select('password').exec(function(err, user){
            if(err) throw err;
            if(!user){
                res.json({
                    statusText:"error",
                    message: "Invalid EmailId"
                });
            }else{
                USER.findOneAndUpdate(user.password, req.body.password, {upsert:false}, function(err, doc){
                    if (err) return res.send(500, { error: err });
                    return res.json({ message: "password updated successfuly"});
                });
            }
        });
    })    
    return api;
};