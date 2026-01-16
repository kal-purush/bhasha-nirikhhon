$('document').ready(function() {
	// local storage usage stuffs
	var localStorageGET = localStorage.getItem('user');
	var localOBJ = JSON.parse(localStorageGET);
	var goalType = localOBJ.goalType
	// The date Setting.

 	for(var i = 0; i < localOBJ.goals.length; i++) {
 		if (localOBJ.goals[i].goalType === 'weight' || localOBJ.goals[i].goalType === 'max') {
 			var tmp = $('<li id="' + i + '"><button class="btn btn-danger">'+localOBJ.goals[i].title+'</button></li>');
 		} else {
 			var tmp = $('<li id="' + i + '"><button class="btn btn-danger">'+localOBJ.goals[i].title+'</button></li>');
	 	}
	 	tmp.on('click', function(){
	 		console.log('about t0 delete')
	 		console.log(this.id)
 			localOBJ.goals.splice(this.id, this.id + 1);
 			console.log(localOBJ.goals);
 			pushUserToLocalStorage(localOBJ);
 			window.location.href = 'profile.html'
	 	});
	 	$('.goalMenu').append(tmp);
 	}

 	function pushUserToLocalStorage(obj) {
		console.log(JSON.stringify(obj), "COOOL");
		localStorage.setItem('user', JSON.stringify(obj));
	}
})