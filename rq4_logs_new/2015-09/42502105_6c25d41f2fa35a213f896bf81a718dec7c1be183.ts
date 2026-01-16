///<reference path="../../reference.ts" />
module application {
	export class HomeCtrl {
		
		//Angular DI
		public static $inject = ['$scope','$location','AppManager','UIManager'];
		
		constructor(
			private $scope: any, 
			private $location,
			private am: gamanager.AppManager,
			private uim: gamanager.UIManager
			
		){
			this.$scope.vm = this;
			console.log('HomeCtrl: Prepared!');
		}
		
		
		/**
		 * Authorize User, load his info aind initialize application
		 * (load Analytics library, get first request for accounts summeries 
		 * and redireft to /accounts)
		 */
		public authorize(){
			console.log('HomeCtrl: Running inicialization.');
		
			//Hide UserInterface  
			this.uim.setLoading(true); 
			
			//Delegate ApplicationManager
			this.am.authorize()
				.then(
					//If OK
					() => {	
						console.log('AppManager: Redirenting at "/accounts" page');
						this.$location.path( '/accounts' ).replace();
						this.$scope.$apply();
						
						//Reset UI
						this.uim.setLoading(false);
					},
					//If error
					(error) => {
						console.error('HomeCtrl: Handling Error:');
						console.log(error);
						//Report Error
						this.uim.alert(error);
						//Reset UI
						this.uim.setLoading(false);
					}
				);
		}
	}
}