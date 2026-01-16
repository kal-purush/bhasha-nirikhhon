(function(){
	'use strict';

	angular
	.module('Widgets')
    .factory('OperationRepository', OperationRepository);
    
    OperationRepository.$inject = ['$http'];

	function OperationRepository($http){
        var repository = {
            getOperationsByOperationType: getOperationsByOperationType
        };

        return repository;

        function getOperationsByOperationType(operationType){
            return $http({

                "method": "GET", 
                "url": "http://localhost:3000/v1/operations/type/" + operationType,
                "data" : {},
                "params" : {
                },
                "headers": {
                }
            }).then(function (response) {
                return response.data;
            })
            .catch(function (err) {
                console.log(err);
                return err;
            });
        }
    }

})();