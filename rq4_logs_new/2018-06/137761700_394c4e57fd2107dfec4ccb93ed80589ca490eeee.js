var chat = document.querySelector('.messages-chat');
var messageInput = document.querySelector('#Message');
GetMess();

// get message 
function GetMess() {
var xhr = new XMLHttpRequest();
xhr.open('GET', 'http://localhost:8000/api/messages',true);
xhr.onload = function() {
    if (xhr.status === 200) {
		var maskey = JSON.parse(xhr.responseText);
		console.log(maskey);
		chat.innerHTML = "";
		for (let i = 0; i < maskey.length ; i++) {
			chat.innerHTML += '<li class="msg-chat"><span class="username-chat">'+maskey[i].user+'</span><span class="message-chat">'+maskey[i].content+'</span></li>'
		}
    }
    else {
        alert('Request failed.  Returned status of ' + xhr.status);
    }
};
xhr.send();
}

messageInput.addEventListener('keydown',function(event){
	if (event.which == 13) {
		SendMess();
	}
})

// post message
function SendMess() {
	console.log('lecul');
	let userName = userNameInput.value;
	let message = messageInput.value;
	console.log(userName);
	console.log(message);
	userNameInput.value = "";
	messageInput.value = "";
	  var xhttp = new XMLHttpRequest();
	  xhttp.open("POST", "http://localhost:3000/oui", true);
	  xhttp.setRequestHeader("Content-type", "application/json");
	  xhttp.send(userName);
	GetMess();
}