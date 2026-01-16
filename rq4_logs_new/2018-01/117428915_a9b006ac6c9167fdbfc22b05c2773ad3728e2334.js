const EVENT_START_YEAR = 618;
const ZOOM_IN_TIMES = 27;

var EventModel = Backbone.Model.extend({
  defaults: {
    id: 0,
    name: "",
    year: 0,
    subcontent: []
  }
});

var EventCollection = Backbone.Collection.extend({
  url: "../data/events.json",
  model: EventModel
});

var EventView = Backbone.View.extend({
  className: "time-line",
  initialize: function(options) {
    //parameters need to be passed in variable options
    this.nextYear = options.nextYear;
  },
  events: {},
  tagName: "div",
  render: function() {
    var height = this.nextYear - this.model.get("year");
    var event = $.parseHTML("<div class='time-line'></div>");
    //var timebar = $.parseHTML("<div class='time-bar'></div>");
    var timetext = $.parseHTML(
      "<div class='time-text'>" +
        "<div class='year'>" +
        this.model.get("year") +
        "</div>" +
        "<div class='name'><div class='firstletter'>" +
        this.model.get("name")[0] +
        "</div><div>" +
        this.model.get("name").substr(1) +
        "</div></div>" +
        "</div>"
    );

    // if (this.model.get("subcontent").length > 0) {
    //   _.each(this.model.get("subcontent"), function(content) {
    //     $(timetext).append("<div class='subcontent'>" + content + "</div>");
    //   });
    // }

    $(event).css("height", height * ZOOM_IN_TIMES + "px");
    //$(timebar).css("height", height * ZOOM_IN_TIMES + "px");
    $(event).append(timetext);
    //$(event).append(timebar);
    this.$el.html(event);
    return this;
  }
});

var EventContainerView = Backbone.View.extend({
  el: ".events-container",
  model: EventModel,
  initialize: function() {
    this.collection.on("sync", this.render, this);
  },
  render: function() {
    var nextYear = this.collection.at(1).get("year");
    this.collection.each(function(model, index) {
      var eventView = new EventView({
        model: model,
        nextYear: nextYear
      });
      this.$el.append(eventView.render().el);
      nextYear = this.collection.at(index + 2).get("year");
    }, this);
  }
});

var eventCollection = new EventCollection();

var eventContainerView = new EventContainerView({
  collection: eventCollection
});
eventCollection.fetch({
  success: function(collection, response) {
    console.log(collection);
    console.log(response);
  }
});