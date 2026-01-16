(function() {
	var httpRequest;
	var ajaxText = document.querySelector('.placeholder'); 
	var ajaxButton = document.querySelector('.wrapper');



	function makeRequest(url) {
		httpRequest = new XMLHttpRequest(); //API thats going to handle all of the ajax stuff. Creates request object
		
		if (!httpRequest) {
			alert("giving up, your browser is way too old");
			return false; //this makes it just quit and user will see alert message
		}

		httpRequest.onreadystatechange = showResult; 
		//this will run multiple times and ask it to call a function 
		httpRequest.open('GET', url); 
		httpRequest.send(); //this is initializing our class - asking it to open a request and passing it a url - we are passing this txt file
		//need to listen for the showResult  function
	}

	function showResult() {
		if(httpRequest.readyState === XMLHttpRequest.DONE) {
			if (httpRequest.status === 200) {
				var response = httpRequest.responseText; 
				ajaxText.innerHTML = response; 
			} else {
				console.log("there was a problem with your request");
			}
		}
	}




	ajaxButton.addEventListener("click", function(){ makeRequest('text.txt');}, false);

})();