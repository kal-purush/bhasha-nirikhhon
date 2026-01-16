/// <reference path="typings/sequelize/sequelize.d.ts" />

import Sequelize = require('sequelize');


function User(db:Sequelize) {
	return db.define('user', {
		id: Sequelize.INTEGER,
		name: Sequelize.STRING,
		age: Sequelize.INTEGER
	});
}

function connect(name: string, file: string) :Sequelize {
	var seq = new Sequelize(name, null, null, {
    dialect: 'sqlite',
    storage: file
	});
	return seq;  
}

export function test() {
	console.log("orm1:", "start...");
	var db = connect("test", "orm1.db");
	db.authenticate();
	var user = User(db);
	db.sync().then(function() {
		return user.create({
			id: 0,
			name: 'test_name',
			age: 18
		});
	}).then(function(d) {
		console.log("orm1:", "get", d.get({plain:true}))
	}) ;
}