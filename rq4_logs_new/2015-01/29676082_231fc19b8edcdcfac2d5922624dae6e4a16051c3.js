/**
 * Created by vijayj on 1/23/2015.
 */
angular.module("todo")
    .controller("MainCtrl", ["$scope", "TodoService", function(scope, TodoService) {
        //console.log(TodoService.todos);

        scope.todos = TodoService.todos;
        scope.add = function() {
            TodoService.add(scope.newTodo);
            scope.newTodo = {};
        }
        scope.delete = function(todo) {
            TodoService.delete(todo);
        }

        scope.clearCompleted = function() {
            TodoService.clearCompleted()
        }
    }]);