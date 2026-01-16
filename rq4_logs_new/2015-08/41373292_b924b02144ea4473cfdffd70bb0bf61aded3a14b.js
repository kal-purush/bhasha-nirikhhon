var myApp = angular.module('dragModule', []);

myApp.directive('myDraggable', ['$document', function($document) {
    return {
      link: function(scope, element, attr) {
	
	var startX = 0, startY = 0, x = 0, y = 0;

		
        element.css({
         cursor: 'pointer'
        });

        element.on('mousedown', function(event) {
          // Prevent default dragging of selected content
	  x = element.prop('offsetLeft');
	  y = element.prop('offsetTop');

          event.preventDefault();
          startX = event.pageX - x;
          startY = event.pageY - y;
          $document.on('mousemove', mousemove);
          $document.on('mouseup', mouseup);
        });

        function mousemove(event) {
          y = event.pageY - startY;
          x = event.pageX - startX;
          element.css({
            top: y + 'px',
            left:  x + 'px'
          });
        }

        function mouseup() {
          $document.off('mousemove', mousemove);
          $document.off('mouseup', mouseup);
        }
      }
    };
  }]);

myApp.controller('PartsLayoutController',['$scope',function($scope){

	$scope.project =
		{
			img:'img1234.png'
		}
	

	$scope.parts = [
		{
			name: 'Sensor Termostático',
			serial: 'XNB009922',
			color: 'lightgreen',
			x: 20,
			y: 10
		}, {
			name: 'Sensor Barômetro',
			serial: 'TTW103922',
			color: 'lightblue',
			x: 80,
			y: 50		
		}, {
			name: 'Sensor Temperatura',
			serial: 'JJK987665',
			color: 'lightgray',
			x: 380,
			y: 160		
		}
	];
}]);


(window.angular);
