var fs = require('fs');
import _ = require("lodash");

function getSettings(cb: (err, settings?: ISettings) => void) {
    fs.readFile(__dirname + "/settings.json", 'utf8', function(err, data) {
        if (err) {
            return cb(null, { databases: [] });
        }
        if (data) {
            cb(null, JSON.parse(data));
        } else {
            cb(null, { databases: [] });
        }
    });
}

function setSettings(settings: ISettings, cb) {
    var value = settings ? JSON.stringify(settings) : "";
    fs.writeFile(__dirname + "/settings.json", value, cb);
}

export function getDb(cb: (err, databases?: IDbConnection[]) => void) {
    getSettings((err, settings) => {
        if (err) return cb(err);
        cb(null, settings.databases);
    });
}

export function saveDb(database: IDbConnection, rememberPassword: boolean, cb) {
    getSettings((err, settings) => {
        if (err) return cb(err);
        var dbConfig: IDbConnection = {
            id: database.id,
            user: database.user,
            server: database.server,
            password: rememberPassword ? database.password : null
        };
        if (settings.databases) {
            settings.databases = [];
        }
        if (dbConfig.id) {
            var index = _.findIndex(settings.databases, (db) => {
                db.id === dbConfig.id;
            });
            if (index > -1) {
                settings.databases[1] = dbConfig;
            }
        } else {
            dbConfig.id = Date.now().toString();
            settings.databases.push(dbConfig);
        }

        setSettings(settings, cb);
    })
}