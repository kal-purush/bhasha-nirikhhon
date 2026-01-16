Events = new Mongo.Collection("", {
  transform: function(someData) {

    // syntax on method to match html display
    someData.display_date = function() {
      return moment(new Date(this.year, this.month, this.day)).format("MMM Do YYYY");
    };

    // syntax on method to match html display
    someData.past_date = function() {
      date = moment();
      recordDate = moment(new Date(this.year, this.month, this.day));
      return recordDate < date;
    };

    return someData;
  }
});

if (Meteor.isClient) {

  Template.body.events({
    "submit .new-events": function (newEvents) {
      newEvents.preventDefault();

      Events.remove({});

      var text = newEvents.target.text.value;
      var eventJSONData = JSON.parse(text);
      newEvents.target.text.value = "";

      _.each(eventJSONData["events"], function(someEvent) {
        Events.insert(someEvent);
      });
    }
  });

  Template.body.helpers({
    events: function () { return Events.find({}, {sort: [["year", "asc"], ["month", "asc"], ["day", "asc"]]}) }
  });
}

if (Meteor.isServer) {
  Meteor.startup(function () {
    // code to run on server at startup
  });
}

