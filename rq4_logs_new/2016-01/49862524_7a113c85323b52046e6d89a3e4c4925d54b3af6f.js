/**
 * @memberOf fp.utils
 */
(function (module) {
  'use strict';

  /**
   * Some state utilities.
   * @constructor StateUtils
   * @param {Object} $q - The Angular $q service.
   * @param {Object} $state - The Angular UI Router $state service.
   */
  function StateUtils($q, $state) {
    var service = this;

    /**
     * Go to a given nested state, making sure to enter parent states too.
     * Be careful, this function mutates the passed `state` array.
     * @private
     * @function goToNested
     * @param {Array} states - An array of state names representing a path.
     * @param {Object} params - Optional parameters to pass to every state.
     * @param {Object} [current] - The current state object.
     * @return {Promise} Resolved with the current state object.
     */
    function goToNested(states, params, current) {
      // Make sure to always return a promise.
      if (!states.length) { return $q.when(current); }
      var name = states.shift();
      var state = current ? '.' + name : name;
      // Pre-bind the function with some arguments, `states` can change later.
      var then = _.partial(goToNested, states, params);
      var isRegular = !$state.get(state, current).abstract;
      // An error would be thrown if attempting to enter an abstract state.
      if (isRegular) { return $state.go(state, params).then(then); }
      if (!states.length) { return $q.reject('abstract state'); }
      // Mutate the `states` array so that we can try to enter the next state.
      states[0] = name + '.' + states[0];
      return then(current);
    }

    /**
     * Sanitize a given states path to a straight from root valid path.
     * @method sanitize
     * @param {String} state - Should point to state using a full path.
     * @return {String}
     */
    service.sanitize = function (state) {
      if (!_.isString(state)) { return ''; }
      return state
        // Remove everything expect letters and dots.
        .replace(/[^a-z.]/ig, '')
        // Regroup multiple consecutive dots.
        .replace(/\.+/g, '.')
        // Remove leading and trailing dots.
        .replace(/^\.|\.$/g, '');
    };

    /**
     * Go to a given state, if nested make sure to enter parent states too.
     * @method goTo
     * @param {String} state - Should point to state using a full path.
     * @param {Object} params - Optional parameters to pass to every state.
     * @return {Promise}
     */
    service.goTo = function (state, params) {
      state = service.sanitize(state);
      if (!state) { return $q.reject('empty state'); }
      return goToNested(state.split('.'), params);
    };
  }

  module.service('stateUtils', ['$q', '$state', StateUtils]);

}(angular.module('fp.utils')));