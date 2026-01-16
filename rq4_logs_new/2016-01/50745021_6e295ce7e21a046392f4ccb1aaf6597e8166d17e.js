angular
	.module('archiveService', ['ngResource'])
	
	.factory('Task', ['$resource', Task])
	
	
function Task($resource) {
	return $resource('api/log-files/tasks/:taskId', {}, {
		query: {method:'GET', params:{phoneId:'phones'}, isArray:true}
	});
};