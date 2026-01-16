(() => {
    'use strict';

    angular.module('shared', []);
    angular.module('store', ['shared', 'ui.router', 'ngMessages']);
    
})();