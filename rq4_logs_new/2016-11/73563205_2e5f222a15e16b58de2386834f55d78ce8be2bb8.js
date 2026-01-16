// js/controllers/main.js

angular.module('assistantsController', [])

    .controller('mainController', function ($scope, $http) {
        // $scope.formData = {};

        // when landing on the page, get all assistants and show them
        $http.get('/assistants')
            .success(function (data) {
                $scope.assistants = data;
            })
            .error(function (data) {
                console.log('Error: ' + data);
            });

        // when submitting the add form, send the text to the node API
        // $scope.createTodo = function () {
        //     $http.post('/api/todos', $scope.formData)
        //         .success(function (data) {
        //             $scope.formData = {}; // clear the form so our user is ready to enter another
        //             $scope.todos = data;
        //         })
        //         .error(function (data) {
        //             console.log('Error: ' + data);
        //         });
        // };

        // delete a todo after checking it
        // $scope.deleteTodo = function (id) {
        //     $http.delete('/api/todos/' + id)
        //         .success(function (data) {
        //             $scope.todos = data;
        //         })
        //         .error(function (data) {
        //             console.log('Error: ' + data);
        //         });
        // };

    });