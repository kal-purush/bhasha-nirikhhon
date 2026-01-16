angular.module('coinpager.service', [
]).factory('coinpager', ['$rootScope',  function ($rootScope) {
 

    var edit = function (employee) {
        //hub.lock(employee.Id); //Calling a server method
    };
    var done = function (employee) {
        //hub.unlock(employee.Id); //Calling a server method
    }

    return {
        editEmployee: edit,
        doneWithEmployee: done
    };
}]);