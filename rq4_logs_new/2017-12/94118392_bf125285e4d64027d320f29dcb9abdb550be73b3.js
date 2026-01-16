/// <reference path="../index.html" />
(function () {
    'use strict';

    var app= angular.module('SlApp.login', ['react']);
        app.controller('LoginController', LoginController);
    LoginController.$inject = ['$scope', '$window'];
    function LoginController($scope, $window) {
        $scope.person = { fname: '', pass: '' };
        $scope.login = function () {
            if ( $scope.person.pass=="1234") {
                $scope.isSuccess = true;
                $scope.spSuccess = "משתמש נכנס בהצלחה";
                $window.location.href="../index.html"

            }
            else {
                $scope.isSuccess = false;

                $scope.spSuccess = "משתמש שגוי";
            }
           
        }
    }
    var Hello = React.createClass({
        propTypes: {
            fname: React.PropTypes.string.isRequired,
            pass: React.PropTypes.string.isRequired
        },


        render: function () {
            return React.DOM.span(null,
              'Hello ' + this.props.fname
            );
        }
    });

    app.value("Hello", Hello);

    app.directive('hello', function (reactDirective) {
        return reactDirective(Hello);
    });
})();