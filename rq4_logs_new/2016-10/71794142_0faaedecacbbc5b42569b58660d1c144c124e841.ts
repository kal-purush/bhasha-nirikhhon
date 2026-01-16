// import * as Sequelize from 'sequelize';
import * as PG from 'pg';
import { Promise } from 'es6-promise'
import path = require('path');
let env = process.env.NODE_ENV || 'development';
let config = require(path.join(__dirname,'..','config','config.json'))[env];

export let CreateDatabasePG = () => {
    var connectString = ('postgres://' + config.username + ':' + config.password + '@' + config.host + '/postgres');

    return new Promise((resolve, reject) => {
        PG.connect(connectString, function (err, client, done) {
            if (err) {
                console.error(err.message);
                reject(err);
                return;
            }

            client.query("SELECT datname FROM pg_database where datname = " + "'" + config.database + "'", function (err1, result) {
                if (err1) {
                    // console.log(err1);
                    reject(err1);
                    return;
                }
                if (result.rows.length == 0) {
                    client.query('Create Database ' + config.database, function (err2, result) {
                        if (err2) {
                            // console.log(err2);
                            reject(err2);
                            client.end();
                        }
                        resolve('Create Database: ' + config.database);
                        client.end();
                        // console.log('Success: Create Database: ' + config.database);
                    });
                }
                else {
                    resolve('Error: ' + config.database + ' exists');
                    client.end();
                    // console.log('Database: ' + config.database + ' exists');
                }

            });
            // client.release();
        });
    });

};

CreateDatabasePG();


// CreateDatabasePG((err, result) => {
//     if (err) {
//         console.log(err);
//     }
//     console.log(result);
// });
