///<reference path="../../typings/tsd.d.ts"/>
module com.test {
    'use strict';

    class TestClass {
        constructor(private $rootScope:ng.IRootScopeService, public $http:ng.IHttpService) {
        }

        test() {
            // try: this.$root... and autocomplete - not working

            // try: this.$h... and autocomplete - working as expected
        }
    }
}