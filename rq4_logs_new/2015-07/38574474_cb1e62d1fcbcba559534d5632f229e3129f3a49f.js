if (Meteor.isClient) {
  Template.navbar.helpers({
    activeIfTemplateIs: function (template) {
      var currentRoute = Router.current();
      return currentRoute &&
        template === currentRoute.lookupTemplate() ? 'active' : '';
    }
  });
}


if (Meteor.isServer) {
  Meteor.startup(function () {
    // code to run on server at startup
  });
}

Router.configure({
  layoutTemplate: 'layout',
  notFoundTemplate: 'index'
});



Router.route('/', function () {
  this.render('index');
});


Router.route('/about', function () {
  this.render('about');
});

Router.route('/services', function(){
	this.render('services');
})

Router.route('/contact', function(){
	this.render('contact');
})

Router.route('/servicesNew', function(){
  this.render('servicesNew');
})