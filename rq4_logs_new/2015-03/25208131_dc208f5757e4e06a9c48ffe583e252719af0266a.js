window.Crack = Ember.Application.create();

Crack.ApplicationAdapter = DS.FirebaseAdapter.extend({
  firebase: new Firebase("https://crack.firebaseio.com/")
});

Crack.ChatAdapter = Crack.ApplicationAdapter.extend({
  pathForType: function(type) {
    return "rooms/general";
  }
});

Ember.Handlebars.helper('date-format', function (date) {
  return moment(date).format('lll');
});

Crack.Router.map(function() {
  this.route('login');
});

Crack.IndexRoute = Ember.Route.extend({
  beforeModel: function () {
    var user = this.controllerFor('application').get('currentUser');
    if (!user) {
      this.transitionTo('login');
    }
  },

  model: function() {
    return this.store.find('chat');
  }
})

Crack.Chat  = DS.Model.extend({
  username : DS.attr('string'),
  message : DS.attr('string'),
  timestamp : DS.attr('date')
})

Crack.ApplicationController = Ember.Controller.extend({
  currentUser:null
});

Crack.IndexController = Ember.ArrayController.extend({
  needs:['application'],
  message:'',

  actions: {
    newChat: function () {
      var username = this.get('controllers.application.currentUser.username');
      var chat = this.store.createRecord('chat', {
        username:username,
        message:this.get('message'),
        timestamp:new Date()
      });
      chat.save();
      this.set('message', '');
    }
  }
});

Crack.LoginController = Ember.ObjectController.extend({
  needs: ['application'],
  username:'',

  actions:{
    logIn:function() {
      console.log(this);
      this.set('controllers.application.currentUser', {
        username:this.get('username'),
      });
      this.transitionToRoute('index');
      this.set('username','');
    }
}})