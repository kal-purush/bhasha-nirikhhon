import { Injectable } from '@angular/core';
import { Message } from '../models/message';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class MessageService {

  private collection = new BehaviorSubject(null);

  constructor() {
    var messages: Array<Message> = [];
    for (let i = 1; i <= 100; i++) {
      var msg = {
        id: i,
        content: `Message #${i} with generated content. This is a message with generated content.`,
        mine_flag: i % 2 == 0
      }
      messages.push(msg);
    }
    this.collection.next(messages);
  }

  public all(): Array<Message> {
    return this.collection.getValue();
  }
}