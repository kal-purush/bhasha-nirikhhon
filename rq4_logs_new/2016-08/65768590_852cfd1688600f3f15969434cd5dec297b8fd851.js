import Ember from 'ember';

export default Ember.Controller.extend({
  myIcon: {
    url: "/assets/images/logo.png"
    // ,
    // size: new google.maps.size(30,30),
    // scaledSize: new google.maps.size(20,20),
    // anchor: new google.maps.point(15, 15),
    // origin: new google.maps.point(0, 0),
    // labelOrigin: new google.maps.point(30, 15)
  },
  actions: {
    handleClick() {
      console.log('handleClick');
    },
    handleDrag() {
      console.log('handleDrag');
    }
  }
});