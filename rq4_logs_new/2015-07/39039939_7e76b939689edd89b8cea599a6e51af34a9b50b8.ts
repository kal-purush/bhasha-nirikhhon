/// <reference path="typings/angular2/angular2.d.ts" />

import {Component, View, bootstrap, NgFor} from 'angular2/angular2';

// Annotation section
@Component({
  selector: 'chat-messages'
})

@View({
  directives: [NgFor],
  template: `
    <ul>
      <li *ng-for="#message of messages">
        {{ message }}
      </li>
    </ul>`
})
class ChatMessages {
  messages: Array<string>;

  constructor() {
    let self = this
    self.messagesRef = new Firebase('https://bearch-chat.firebaseio.com/messages');
    self.messagesRef.on('child_added', function(snapshot) {
      self.messages.push(snapshot.val())
    });
    self.messages = ['First message', 'Second message'];
  }
}

bootstrap(ChatMessages);