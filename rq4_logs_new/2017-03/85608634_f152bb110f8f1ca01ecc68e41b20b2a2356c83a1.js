(function (angular){
    'use strict';

    todoService.$injector = ['$http', '$q']

    function todoService ($http, $q){

        this.get = function () {
            var defer = $q.defer();
            $http({
                method: 'GET',
                url: "http://www.mocky.io/v2/58088826100000e9232b75b0",
                headers: {
                    'Access-Control-Allow-Origin': 'http://www.mocky.io/',
                }
            }).then(function(response){
                defer.resolve(response.data)
            }).catch(function(response){
                defer.reject(response);
                console.error('api response error', response)
            })
            return defer.promise;
        }
    }

    angular
        .module('todoApp')
        .service('todoService', todoService)
})(angular);