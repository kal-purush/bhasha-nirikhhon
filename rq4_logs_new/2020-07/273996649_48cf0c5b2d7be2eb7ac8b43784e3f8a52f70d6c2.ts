import { Injectable } from '@angular/core';
import { ChannelService } from './channel.service';
import { map } from 'rxjs/operators';
import { Message, MessageHistory } from '../models/message';

@Injectable({
  providedIn: 'root'
})
export class MessageService {

  private collection: Array<MessageHistory> = [];

  constructor(
    private chn: ChannelService
  ) { }

  public find(channelID: number) {
    return this.chn.find(channelID).pipe(
      map(foundChannel => {
        if (!foundChannel) {
          return null;
        }
        var foundHistory: MessageHistory = this.collection.find(e => e.id == channelID);
        if (!foundHistory) {
          foundHistory = {
            id: foundChannel.id,
            channel: foundChannel,
            lastFetch: new Date().getTime(),
            history: this.messageMockup()
          }
          this.collection.push(foundHistory);
        }
        return foundHistory;
      })
    );
  }

  private messageMockup() {
    var messages: Array<Message> = [];
    var limit = Math.floor(Math.random() * 100);
    for (let i = 1; i <= limit; i++) {
      var msg = {
        id: i,
        content: `Message #${i} with generated content. This is a message with generated content.`,
        mine_flag: Math.floor(Math.random() * 2) == 1
      }
      messages.push(msg);
    }
    return messages;
  }
}