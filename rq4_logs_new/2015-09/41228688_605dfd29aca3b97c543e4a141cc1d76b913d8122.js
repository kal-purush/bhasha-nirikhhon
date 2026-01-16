console.log('Real Time');
function MainViewModel(data) {
	var self = this;
	var robot = io('http://127.0.0.1:3000/api/robots/chappie');
	
	self.lineChartData = ko.observable({
		labels : ["","","","Gen","","","ºC", ""],
		datasets : [
			{
				fillColor : "rgba(151,187,205,0.5)",
				strokeColor : "rgba(151,187,205,1)",
				pointColor : "rgba(151,187,205,1)",
				pointStrokeColor : "#fff",
				data : [65,59,90,81,56,55,40]
			}
		]
	});
	
	setInterval(function() {
        robot.emit('checkGeneric');
    }, 100);

	robot.on('genericValue', function(analogValue){
		// console.log(analogValue);
		var generic = ((analogValue*100)/1023).toFixed(2);
		console.log(generic);
		self.lineChartData().datasets[0].data.shift();
		self.lineChartData().datasets[0].data.push(generic);
		
		self.initLine();
	});
	
	self.initLine = function() {
		var options = {
			animation : false,
			scaleOverride : true,
			scaleSteps : 20,//Number - The number of steps in a hard coded scale
			scaleStepWidth : 5,//Number - The value jump in the hard coded scale				
			scaleStartValue : 0//Number - The scale starting value
		};
		
		var genericctr = $("#genericRTchart").get(0).getContext("2d");
		var myLine = new Chart(genericctr).Line( vm.lineChartData(), options );
	}
	
}

var vm = new MainViewModel();
ko.applyBindings(vm);
vm.initLine();