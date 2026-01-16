(function(){
	'use strict';

	angular.module('profile').controller('ProfileController', ['ProfileService', ProfileController]);

	function ProfileController(service) {
		var self = this;

		self.profile = service.loadProfile();
		self.skillReadonly = true;
	}

})();