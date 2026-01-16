
var User = require('../models/user.js');
var List = require('../models/list.js');

// Simple controller for creating and updating list on user object
var apiController = {

  // Handle incoming requests for the '/user/lists' route
  createList: function(req, res){
  	var newList = new User.user(req.body)
	newList.save(function(err, doc){
		res.send(doc);
	})
    // res.post(req.user)
  },

  {
  // Handle incoming requests for the '/user/updateList' route
  updateList: function(req, res){
  	var updateList = User.user(req.body)
  	updateList.save(function(err, doc){
  		res.send(req.user);
  	})
  	}
  }

  // ,
  // templates: function(req, res){
  //   res.render('templates/' + req.params.templateName, {
  //   user: req.user  
  //   } )
  //   // res.send(req.user)
  // }

};

// Export our index control
module.exports = listController;