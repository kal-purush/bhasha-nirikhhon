import {Component, View, bootstrap, NgFor} from 'angular2/angular2';
import {FirebaseEventPipe} from './firebasepipe';

@Component({
	selector: 'display',
	template: `
	  	<div>
		  <button [hidden]="isLoggedIn" class="twitter" (click)="authWithTwitter()">Sign in with Twitter</button>

		</div>
	  <div class="message-input">
	  	<input [hidden]="!isLoggedIn" #messagetext (keyup)="doneTyping($event)" placeholder="Enter a message...">
	  </div>
	  <ul class="messages-list">
		  <li *ng-for="#message of firebaseUrl | firebaseevent:'child_added'">
		  	<strong>{{message.name}}</strong>: {{message.text}}
		  </li>
	  </ul>
	`,
	directives: [NgFor],
	pipes: [FirebaseEventPipe]
})
class MessageList {
	firebaseUrl: string;
	messagesRef: Firebase;
	isLoggedIn: boolean;
	authData: any;
	constructor() {
		this.firebaseUrl = "https://angular-connect.firebaseio.com/messages";
		this.messagesRef = new Firebase(this.firebaseUrl);
		this.messagesRef.onAuth((user) => {
			if (user) {
				this.authData = user;
				this.isLoggedIn = true;
			}
		});
	}
	
	authWithTwitter() {
		this.messagesRef.authWithOAuthPopup("twitter", (error) => {
			if (error) {
				console.log(error);
			}
		});
	}

	doneTyping($event) {
	  if($event.which === 13) {
	    this.addMessage($event.target.value);
	    $event.target.value = null;
	  }
	}
	
	addMessage(message: string) {
		var newString = message;
		this.messagesRef.push({
			name: this.authData.twitter.username,
			text: newString
		});
	}
}

bootstrap(MessageList);