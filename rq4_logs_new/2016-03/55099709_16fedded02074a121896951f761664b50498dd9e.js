import Ember from 'ember';

export default Ember.Component.extend({
  activeStep: null,

  steps: [],

  didInsertElement() {
    this._super(...arguments);

    this.set('steps', Ember.A());
  },

  registerStep(step) {
    var steps = this.get('steps');
    steps.pushObject(step);
  },

  completeStep(step) {
    let activeStep = this.get('activeStep');
    let nextStep = activeStep + 1;

    step.set('completed', true);
    this.set('activeStep', nextStep);
  },

  activateStep(step) {
    this.set('activeStep', step.get('stepNumber'));
  }
});