import * as pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';
let env = process.env.NODE_ENV || 'development';
let config = require(path.join(__dirname, '..', 'config', 'database.config.json'))[env];

let sql = fs.readFileSync(path.join(__dirname, '..', '..', 'doc', 'script.sql')).toString();

pg.connect('postgres://' + config.username + ':' + config.password + '@' + config.host + '/' + config.database, (err, client, done) => {
    if (err) {
        console.error('error:', err);
        process.exit(1);
    }

    client.query('CREATE SCHEMA IF NOT EXISTS ' + config.schema + ';')
        .then(() => {
            client.query(sql, (err, result) => {
                done();
                if (err) {
                    console.error('error: ', err)
                    process.exit(1);
                }
                process.exit(0);
            });
        })
        .catch(err => {
            done();
            console.error('error: ', err);
            process.exit(1);
        })


});