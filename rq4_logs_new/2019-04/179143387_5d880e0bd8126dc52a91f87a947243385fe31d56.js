/**
 * Created by Shivam Raj.
 * Project: gamingmonk
 */
//validation
const validators = require('../validators/user_and_company');
//middleware
const middleware = require('../middlewares/user');
//handler
const Handlers = require('../handlers/user_handler');

//dependencies
const Joi = require('joi');
Joi.objectId = require('joi-objectid')(Joi);

exports.login_page = {
	tags: [ 'api' ],
	notes: 'LogIn Page',
	description: 'LogIn Page',
	handler: async (request, h) => {
		return h.view('login', { title: 'LogIn' });
	}
};

exports.home = {
	auth: 'jwt',
	tags: [ 'api' ],
	notes: 'home Page',
	description: 'home Page',
	handler: async (request, h) => {
		return h.view('index', { title: 'welcome to Home Page' });
	}
};

exports.signup_page = {
	tags: [ 'api' ],
	notes: 'SignUp Page',
	description: 'SignUp Page',
	handler: async (request, h) => {
		return h.view('signup', { title: 'SignUp' });
	}
};

exports.signup = {
	tags: [ 'api' ],
	notes: 'Sign up',
	validate: {
		payload: validators.signup
	},
	pre: [ { method: middleware.checkEmailExistance, assign: 'User' } ],
	handler: Handlers.signup
};

exports.login = {
	tags: [ 'api' ],
	notes: 'Log In',
	validate: {
		payload: validators.login
	},
	handler: Handlers.login
};

exports.logout = {
	auth: 'jwt',
	tags: [ 'api' ],
	notes: 'Log Out',
	handler: Handlers.logout
};