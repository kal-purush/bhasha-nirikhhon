import Ember from 'ember';

export default Ember.Component.extend({
  classNames: ['google-map'],

  latLng: function () {
  	return new google.maps.LatLng(this.get('latitude'), this.get('longitude'));
  }.property('latitude', 'longitude'),
  
  mapOptions: function () {
  	return { zoom: 13, center: this.get('latLng')}
  }.property('latitude', 'longitude'),

  map: function () {
  	var el = document.getElementById(this.get('elementId'));
  	return new google.maps.Map(el, this.get('mapOptions'));
  }.property('latitude', 'longitude'),

  marker: function () {
  	var options = { position: this.get('latLng'), map: this.get('map') }
  	return new google.maps.Marker(options);
  }.property('latitude', 'longitude'),

  drawMap: function () {
  	this.get('marker');
  }.observes('latitude', 'longitude'),

  didInsertElement: function () {
  	if (this.get('latitude') && this.get('longitude')) {
  		this.drawMap();
  	}
  }
});