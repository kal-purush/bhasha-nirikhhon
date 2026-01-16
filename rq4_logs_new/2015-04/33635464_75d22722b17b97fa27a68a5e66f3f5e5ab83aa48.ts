///<reference path='Game.ts' />
///<reference path='../defs/socket.io-client/socket.io-client.d.ts' />

module Chat {
	
	var chatForm: HTMLElement;
	var chatInput: HTMLInputElement;
	var chatMessages: HTMLElement;
	
	export function init() {
		chatForm = document.getElementById('chat-form');
		chatInput = <HTMLInputElement> document.getElementById('chat-input');
		chatMessages = document.getElementById('chat-messages');
	}

	export function print(text:string) {
		var div = document.createElement('div');
		div.textContent = text;
		chatMessages.appendChild(div);
	}
	
	export function register(socket:SocketIOClient.Socket) {
		chatForm.onsubmit = function (event:Event) {
			socket.emit('message', chatInput.value);
			chatInput.value = '';
			chatMessages.scrollTop = chatMessages.scrollHeight;
			event.preventDefault();
		};
		
		socket.on('message', function (text) {
			print(text);
		});
	}
	
}