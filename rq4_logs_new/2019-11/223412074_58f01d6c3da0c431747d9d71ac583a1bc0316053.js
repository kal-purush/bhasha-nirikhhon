import Component from '@ember/component';
import { action } from '@ember/object';

export default Component.extend({
  isHover: false,

  didInsertElement() {
    this._super(...arguments);
    this.element.addEventListener('mouseenter', this.setHover);
    this.element.addEventListener('mouseleave', this.unsetHover);
  },

  willDestroyElement() {
    this._super(...arguments);
    this.element.removeEventListener('mouseenter', this.setHover);
    this.element.removeEventListener('mouseleave', this.unsetHover);
  },

  setHover: action(function() {
    console.log('setHover');
    this.set('isHover', true);
  }),

  unsetHover: action(function() {
    console.log('unsetHover');
    this.set('isHover', false);
  })
});