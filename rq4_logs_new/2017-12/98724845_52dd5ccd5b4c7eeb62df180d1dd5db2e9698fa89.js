var express = require('express'),
	router = express.Router(),
	mongoose = require('mongoose'),
	slug = require('slug'),
	Post = mongoose.model('Post'),
	Category = mongoose.model('Category'),
	User = mongoose.model('User'),
	pinyin = require('pinyin');
var auth = require('./user'),
	category = require('./category');
module.exports = function (app) {
	app.use('/admin/manager', router);
};
router.get('/users',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/user');
});
router.get('/managers',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/manager')
});
router.get('/categories',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/category')
});
router.get('/comments',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/comment')
});
router.get('/blogs',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/blog')
});
router.get('/chart',auth.requireLogin,function(req,res,next){
	res.render('admin/manager/chart')
});