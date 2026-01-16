var app = angular.module('boots_cats', [])

app.factory('trackData', ['$http', function( $http ) {
	return {
		get : function() {
			var self = this;
			var id = Cookies.get("loggedinId");

			$http.get('/users/' + id + '/tracks').then(function( response ){
				console.log(self);
				self.tracks = response.data;
			});
		}

			// post : function() {
			// 	var self = this;
			// 	$http.post('/players', {
			// 		// console.log(self);
			// 		name: this.formPlayerName, 
			// 		position: this.formPlayerPosition,
			// 		imgURL: this.formPlayerImg
			// 	}).then(function success(response) {
			// 		self.players.push(response.data);
			// 		self.formPlayerName = '';
			// 		self.formPlayerPosition = '';
			// 		self.formPlayerImg = '';
			// 	}, function error() {
			// 		console.log('error');
			// 	});
			// },

			// put : function(id){
	            
	  //     var self = this;
	  //       $http.put('/players/' + id, { 
	  //         name: this.formPlayerName, 
	  //         position: this.formPlayerPosition,                
	  //         imgURL: this.formPlayerImg
	  //       }).then(function success (response) {

	  //         // Empty form
	  //         self.formPlayerName = "";
	  //         self.formPlayerPosition = "";
	  //         self.formCoasterImgURL = "";
	  //       }, function error() {
	  //         console.log('error');
	  //       });
	  //   },

	  //   delete : function(id){
	  //   	var self = this;
	  //   	$http.delete('/players/' + id).then(function success(response){
	  //   		self.get();
	  //   	}, function error(){
	  //   		console.log('error');
	  //   	});
	  //   }
	}
}]);

app.directive('tracks', function() {
	return {
		controllerAs: 'trackCtrl',
		controller: ['trackData', function trackCtrl(trackData) {
			this.trackData = trackData;

			var self = this;

			this.getTracks = function() {
				console.log("getting tracks");
				self.trackData.get();
			}

		}] // close controller
	} // close return object
}) // close angular module