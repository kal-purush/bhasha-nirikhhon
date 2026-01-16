'use strict';

angular.module('ponyApp').controller('DevController',
		[ '$scope', '$route', 'TimeService', 'CalendarService', function($scope, $route, TimeService, CalendarService) {
		  var self = this;
		  
			$scope.curTimes = [ {
				name : 'Total Days',
				value : CalendarService.elapsedDaysx
			}, {
				name : 'Is night?',
				value : CalendarService.isNight
			}, {
				name : 'Current Season',
				value : CalendarService.currentSeason()
			}, {
				name : 'Total Years',
				value : CalendarService.elapsedYears
			} ];

		  this.updateDay = function(value) {
		  	$scope.curTimes[0].value = value;
		  }
		  
			$scope.buttonText = "Start";

			$scope.start = function() {
				if (!TimeService.isStarted()) {
					$scope.buttonText = "Pause";
					TimeService.start();
				}
				else if (TimeService.isPaused()) {
					$scope.buttonText = "Pause";
					TimeService.unpause();
				} else {
					$scope.buttonText = "Unpause";
					TimeService.pause();
				}
			}
			
		  // Self initialization
		  $scope.init = function () {
				CalendarService.init();
				CalendarService.subscribeDay(self);
				//CalendarService.subscribeCycle(self);
				//CalendarService.subscribeSeason(self);
				//CalendarService.subscribeYear(self);
		  }
		 
		  $scope.init();
		} ]);