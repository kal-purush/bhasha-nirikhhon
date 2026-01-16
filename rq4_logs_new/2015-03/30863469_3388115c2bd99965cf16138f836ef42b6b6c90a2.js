import Ember from 'ember';

export default Ember.Component.extend({
  classNames: ['image-actions'],

  actions: {
    rotateRight: function() {
      this.sendAction('rotateRightAction');
    }
  }
});