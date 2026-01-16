angular.module('MyTasksApp', [
    'officeuifabric.core',
    'officeuifabric.components'
    ]).controller('MyTasksController', function ($http) {
        var myTasks = this;

        var onTaskComplete = function (response) {
            myTasks.taskWithId1 = response.data;
        }

        $http.get("http://localhost:57600/api/tasks/1")
            .then(onTaskComplete);
    });