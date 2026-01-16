


function toggleStyles(){
	var elementsToToggle = {

	 	messageCard		: document.getElementById("message-card"),
	 	deleteButton	: document.getElementById("delete-button"),
	 	messageDisplay	: document.getElementById("message-display"),
	 	messageWrapper	: document.getElementById("message-wrapper"),
	 	navElement		: document.getElementById("nav-element"),
	 	clearButton		: document.getElementById("clear-button"),
	 	newMessage      : document.getElementById("new-message"),
	 	largeText		: document.getElementById("text-message")
	};

	for (element in elementsToToggle) {
		// We grab the id, because we're gonna use it in each
		// class name for the dark theme.
		//
		console.log('--stuff in our loop--');
		console.log('togglingElements')
		var id = element.id;
		console.log(id)

		// Now we toggle the theme from light to dark.
		// 
		console.log(element.classList);
		element.classList.toggle(`${id}-dark`);
	}
}