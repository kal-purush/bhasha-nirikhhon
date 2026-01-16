(function() {
    "use strict";

    var usStates = {
        AL: "ALABAMA",AK: "ALASKA",AS: "AMERICAN SAMOA",AZ: "ARIZONA",AR: "ARKANSAS",CA: "CALIFORNIA",CO: "COLORADO",
        CT: "CONNECTICUT",DE: "DELAWARE",DC: "DISTRICT OF COLUMBIA",FM: "FEDERATED STATES OF MICRONESIA",FL: "FLORIDA",
        GA: "GEORGIA",GU: "GUAM",HI: "HAWAII",ID: "IDAHO",IL: "ILLINOIS",IN: "INDIANA",IA: "IOWA",KS: "KANSAS",KY: "KENTUCKY",
        LA: "LOUISIANA",ME: "MAINE",MH: "MARSHALL ISLANDS",MD: "MARYLAND",MA: "MASSACHUSETTS",MI: "MICHIGAN",MN: "MINNESOTA",
        MS: "MISSISSIPPI",MO: "MISSOURI",MT: "MONTANA",NE: "NEBRASKA",NV: "NEVADA",NH: "NEW HAMPSHIRE",NJ: "NEW JERSEY",
        NM: "NEW MEXICO",NY: "NEW YORK",NC: "NORTH CAROLINA",ND: "NORTH DAKOTA",MP: "NORTHERN MARIANA ISLANDS",OH: "OHIO",
        OK: "OKLAHOMA",OR: "OREGON",PW: "PALAU",PA: "PENNSYLVANIA",PR: "PUERTO RICO",RI: "RHODE ISLAND",SC: "SOUTH CAROLINA",
        SD: "SOUTH DAKOTA",TN: "TENNESSEE",TX: "TEXAS",UT: "UTAH",VT: "VERMONT",VI: "VIRGIN ISLANDS",VA: "VIRGINIA",
        WA: "WASHINGTON",WV: "WEST VIRGINIA",WI: "WISCONSIN",WY: "WYOMING"};

    /*
     * Here we create the module that is referenced by the ng-app directive.
     *
     * See https://docs.angularjs.org/api/ng/function/angular.module
     */
    angular.module('contactApp', ['file-data-url', 'ngRoute', 'LocalStorageModule', 'ngMap'])
        .config(function($routeProvider, $locationProvider) {
            $routeProvider.when('/', {
                templateUrl: 'views/contactList.html',
                controller: 'contactListController'
            }).when('/contact', {
                templateUrl: 'views/contact.html',
                controller: 'contactController'
            }).when('/contact/:id', {
                templateUrl: 'views/contact.html',
                controller: 'contactController'
            });
            $locationProvider.html5Mode(false);
            $locationProvider.hashPrefix('!');
        })
        .config(function (localStorageServiceProvider) {
            localStorageServiceProvider
                .setPrefix('08724.hw5');
        })
        .controller('contactListController', function($scope, localStorageService, $location) {
            $scope.localStorage = localStorageService;

            /*
             * Add a behavior to handle when the add contact is clicked
             */
            $scope.add = function() {
                $location.path('/contact');
            };

            /*
             * Add a behavior to handle when the name is clicked
             */
            $scope.update = function(key) {
                $location.path('/contact/' + key);
            }

            /*
             * Add a behavior to handle when the remove X is clicked
             */
            $scope.remove = function(key) {
                localStorageService.remove(key);
            }
        })
        .controller('contactController', function($scope, localStorageService, $routeParams, $location) {
            $scope.stateOptions = usStates; //Available via closure
            var key = $routeParams.id;
            if (key) {
                $scope.contact = localStorageService.get(key);
            } else {
                $scope.contact = null;
            }
            /*
             * Add a behavior to handle when the save is clicked
             */
            $scope.save = function() {
                if (!key) {
                    // generate key by name
                    key = $scope.contact.firstName + "_" + $scope.contact.lastName;
                }
                if (key !== ($scope.contact.firstName + "_" + $scope.contact.lastName)) {
                    // name is modified
                    localStorageService.remove(key);
                    key = $scope.contact.firstName + "_" + $scope.contact.lastName;
                }
                localStorageService.set(key, $scope.contact);
                $location.path('/');
            };
        });

})();