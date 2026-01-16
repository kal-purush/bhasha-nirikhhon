// public/core.js

angular.module('scratchApp.services', []);
angular.module('scratchApp.controllers', ['scratchApp.services']);
var scratch = angular.module('scratchApp', ['scratchApp.controllers','scratchApp.services']);

angular.module('scratchApp.controllers').controller('mainController', ['$scope', '$http', function ($scope, $http) {

    $http.get('/api/nodes')
        .success(function(data) {

            $scope.nodes = data;
            console.log(data);
        })
        .error(function(data) {
            console.log('Error: ' + data);
        });

    // when submitting the add form, send the text to the node API
    /*
    $scope.createTodo = function() {
        $http.post('/api/todos', $scope.formData)
            .success(function(data) {
                $scope.formData = {}; // clear the form so our user is ready to enter another
                $scope.todos = data;
                console.log(data);
            })
            .error(function(data) {
                console.log('Error: ' + data);
            });
    };*/
}]);