import Ember from 'ember';

export default Ember.Component.extend({

  addLike(food) {
    food.incrementProperty('likes');
    food.save()
  },

  disLike(food) {
    food.decrementProperty('likes');
    food.save()
  },
});