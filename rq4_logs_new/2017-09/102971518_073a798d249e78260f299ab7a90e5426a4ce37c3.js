var inquirer = require('inquirer');
var mysql = require('mysql');

var user;

var connection = mysql.createConnection({
    host: "localhost",
    port: 3306,

    // Your username
    user: "root",

    // Your password
    password: "",
    database: "great_bayDB"
});

exports.login = function(){
    inquirer.prompt(
        {
            name: 'login',
            type: 'list',
            message: 'Please Login',
            choices: ['Login', 'Create an account']
        }
    ).then(function(result){
        if(result.login === 'Login'){
            inquirer.prompt([
                {
                    name: 'username',
                    type: 'input',
                    message: 'Username'
                },
                {
                    name: 'password',
                    type: 'password',
                    message: 'Password'
                }
            ]).then(function(result){
                connection.query(
                    'SELECT * FROM USERS WHERE username=?', [result.username], function(err, res){
                        console.log(res[0]);
                        if(result.password === res[0].password){
                            console.log('Welcome ' + res[0].username);
                            user = res.username;
                        } else {
                            console.log('Password incorrect');
                            exports.login();
                        }
                    }
                )
            })
        } else if (result.login === 'Create an account'){
            inquirer.prompt([
                {
                    name: 'username',
                    type: 'input',
                    message: 'Desired username'
                },
                {
                    name: 'password',
                    type: 'password',
                    message: 'Type your password'
                },
                {
                    name: 'confirm',
                    type: 'password',
                    message: 'Retype password'
                }
            ]).then(function(result){
                var userExists;
                connection.query("SELECT * FROM users", function(err, res) {
                    if(!err){
                        for(i = 0; i < res.length; i++){
                            if(res[i].username === result.username){
                                console.log('Username is taken');
                                exports.login();
                            } else {
                                if(result.password !== result.confirm){
                                    console.log('Passwords did not match');
                                } else {
                                    connection.query(
                                        "INSERT INTO users SET ?",
                                        {
                                            username: result.username,
                                            password: result.password
                                        }
                                    );
                                    console.log('Account created now login');
                                    exports.login();
                                }
                            }
                        }
                    } else {
                        console.log(err)
                    }
                });

            })
        }
    });

};

exports.login();