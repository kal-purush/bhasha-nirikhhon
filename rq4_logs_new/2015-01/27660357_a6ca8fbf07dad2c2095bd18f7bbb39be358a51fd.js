import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'li',

  label: function() {
    if (this.get('retailers')) {
      return this.get('return.entity.name');
    }
    else {
      return this.get('return.issue.title');
    }
  }.property('retailers', 'return.issue.title', 'return.entity.name')
});