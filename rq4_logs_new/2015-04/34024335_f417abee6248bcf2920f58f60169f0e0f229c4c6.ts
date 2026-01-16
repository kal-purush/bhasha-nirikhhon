/// <reference path="typings/meteor/meteor.d.ts"/>

if (Meteor.isClient) {
  // counter starts at 0
  Session.setDefault('counter', 0);

  var helloTemplate: Template = Template["hello"];
  helloTemplate.helpers({
    counter: function () {
      return Session.get('counter');
    }
  });

  helloTemplate.events({
    'click button': function () {
      // increment the counter when button is clicked
      Session.set('counter', Session.get('counter') + 1);
    }
  });
}

if (Meteor.isServer) {
  Meteor.startup(function () {
    // code to run on server at startup
  });
}