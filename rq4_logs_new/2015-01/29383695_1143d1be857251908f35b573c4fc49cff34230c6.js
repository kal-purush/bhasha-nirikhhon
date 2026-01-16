
var mongoose = require('mongoose');
var bcrypt = require('bcrypt-nodejs');
var UserDB = require('../models/User');
//var 

module.exports = User;

function User(obj){
	for(var key in obj){

		this[key] = obj[key];

	}


}

User.prototype.save = function(fn){

	if(this._id){
		this.update(fn);
	}else{
		//var user = this;
		// fn('no id');
		UserDB.create({

			name: this.name,
			pass: this.pass,
			age: this.age

		}, function(err){

			if(err) return fn(err);
			console.log('successfully created!');

		})
	}

}

User.prototype.update = function(fn){

	var user = this;
	console.log('user : ' + user.pass);
	console.log('update fn');
	UserDB.update({_id: user._id}, {$set: {

		name : user.name,
		pass : user.pass,
		age : user.age

	}}, function (err) {
		if(err) return fn(err);
		console.log('successfully updated!!');
	});
}

User.getByName = function(name, fn){
	User.getId(name, function(err, id){
		console.log('user id : ' + id);
		if(err) return fn(err);
		User.get(id, fn);
	});
}

User.getId = function(name, fn){

	UserDB.findOne({name : name}, function(err, user){
		console.log('find : ' + user);
		if(err) return fn(err);
		if(!user){
			fn();
		}else{
		fn(err, user._id);
		}
	});
}

User.get = function(id, fn){

	UserDB.findById(id, function(err, user){

		if(err) return fn(err);
		console.log('get : ' + user);
		fn(null, user);

	});

}

User.authenticate = function(name, pass, fn){

	User.getByName(name, function(err, user){

		if(err) return fn(err);

		if(!user._id) return fn();

		if(user.pass != pass){
			console.log('wrong password');
			fn();
		}else{
			fn(err, user);
		}

	});

}