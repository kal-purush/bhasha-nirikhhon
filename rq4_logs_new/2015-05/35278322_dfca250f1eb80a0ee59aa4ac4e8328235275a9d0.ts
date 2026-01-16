import fs = require('fs');
import path = require('path');
import Sequelize = require('sequelize');
import ReCalLib = require("../lib/lib");

var basename = path.basename(module.filename);
var env = process.env.NODE_ENV || 'development';
var sequelize = new Sequelize(process.env.DATABASE_URL, {
    define: {
        allowNull: false,
    },
    logging: Number(process.env.SEQUELIZE_LOG) ? console.log : ()=>{},
});
var db: ReCalLib.Interfaces.DatabaseProxy = {
    sequelize: sequelize,
};

fs.readdirSync(__dirname).filter(function(file) {
    return (file.indexOf('.') !== 0) && (file !== basename) && path.extname(file) === ".js";
}).forEach(function(file) {
    var model = sequelize.import(path.join(__dirname, file));
    db[model.name] = model;
});

Object.keys(db).forEach(function(modelName) {
    if (modelName === 'sequelize') {
        return;
    }
    if ('associate' in db[modelName]) {
        db[modelName].associate(db);
    }
});

let invariants = [
    ReCalLib.Invariants.notNullOrUndefined(db.Task),
    ReCalLib.Invariants.notNullOrUndefined(db.TaskInfo),
    ReCalLib.Invariants.notNullOrUndefined(db.TaskGroup),
    ReCalLib.Invariants.notNullOrUndefined(db.User),
];

ReCalLib.Invariants.checkArray(invariants);

export = db;