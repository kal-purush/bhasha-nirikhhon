window.onload = function() {
	var numWorkers = 3;
	var workerArray = [];
	for(var i = 0; i<numWorkers;i++) {

		workerArray[i] = new Worker("worker.js");
		workerArray[i].onmessage = function(event) {
			var message = "worker says " + event.data;
			document.getElementById("output").innerHTML += message + "<br>";
		}
	}
	for(var i = 0; i< numWorkers; i++)		
		workerArray[i].postMessage("ping");

}