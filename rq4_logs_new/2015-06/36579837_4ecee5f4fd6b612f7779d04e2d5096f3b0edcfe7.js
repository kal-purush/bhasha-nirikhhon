import Ember from 'ember';

export
default Ember.Component.extend({
  isRating: function() {
    return !!this.get('label').match(/rating/i);
  }.property('label')
});