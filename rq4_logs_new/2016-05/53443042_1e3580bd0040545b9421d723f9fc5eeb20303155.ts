import {Injectable} from 'angular2/core'
import {Conversation} from './../models/Conversation'

declare var BABLOT_API_URL: any;
declare var fetch: any;
declare var require: any;
declare var Promise: any;

@Injectable()
export class ConversationHistory {
  constructor(){
  }

  getOne(fingerprint: string){
    return new Promise((resolve, reject) => {
      resolve(this.allData[0]);
    });
  }

  getPreviewList(){
    return new Promise((resolve, reject) => {
      fetch(`${BABLOT_API_URL}/conversation/preview`).then((res) => {
        return res.json();
      }).then((previews)=> {
        console.log(previews);
        resolve(previews.data)
      }).catch((err)=> {
        console.log(err);
      });
    });
  }

  loadMessageHistory(conversation){
    return fetch(`${BABLOT_API_URL}/conversation/${conversation.roomId}`).then((res) => {
      return res.json();
    }).then((rawConversation) => {
      console.log(rawConversation);
      var messages = rawConversation.messages.map((message)=> {
        return {
          body: message.Body,
          FromFingerprint: message.FromFingerprint
        }
      });

      conversation.messages = messages;
    }).catch((err)=> {
      console.log(err);
    });
  }
}