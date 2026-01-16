var counter = 0;
var limit = 100;


var textToSend = "Me Responde!";

var i = setInterval(function() {
	window.InputEvent = window.Event || window.InputEvent;
	var d = new Date();
	var event = new InputEvent('input', {
		bubbles: true
	});
	var textbox = document.querySelector('#main > footer > div.block-compose > div.input-container > div.pluggable-input.pluggable-input-compose > div.pluggable-input-body.copyable-text.selectable-text');
	
	// O envio para quando a pessoa responde, caso queira remover, basta apagar essa condiço. 
	if ( counter > 0 ) {
		var lastMessage = document.querySelector('#main > .pane-body > .copyable-area > .pane-chat-msgs .msg:last-child > .message');

		if ( lastMessage.classList.contains('message-in') ) {
			textToSend = "Finalmente respondeu!";
			clearInterval(i);
		}
	}

	textbox.textContent = textToSend;
	textbox.dispatchEvent(event);
	document.querySelector("button.compose-btn-send").click();
	counter++;

	if (counter === limit && limit !== 0)
		clearInterval(i);

	console.log("Já enviei " + counter + ' mensagens.')

}, 1000); // 1000 = 1 segundo, tempo que será enviada cada mensagem. 