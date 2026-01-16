/**
 * LoadingDirective.ts
 *
 */

/// <reference path="../_references.ts" />

module IMCV.Koluki {
    "use strict";

    import IAttributes = angular.IAttributes;
    import IAugmentedJQuery = angular.IAugmentedJQuery;
    import IRootScopeService = angular.IRootScopeService;
    import IScope = angular.IScope;

    /**
     * Loading directive.
     *
     */
    export class LoadingDirective {

        private _$rootScope: IRootScopeService;

        public static $inject = [
            "$rootScope"
        ];

        /**
         * Creates an instance of loading directive.
         *
         * @constructor
         * @param $rootScope The root scope.
         */
        constructor($rootScope: IRootScopeService) {
            this._$rootScope = $rootScope;
        }

        public restrict = "E";

        public templateUrl = "views/common/loading.html";

        public link = (scope: ILoadingScope, element: IAugmentedJQuery, attributes: IAttributes) => {
            scope.isLoading = false;

            this._$rootScope.$on("$routeChangeStart", () => {
                scope.isLoading = true;
            });

            this._$rootScope.$on("$routeChangeSuccess", () => {
                scope.isLoading = false;
            });
        }

    }

}