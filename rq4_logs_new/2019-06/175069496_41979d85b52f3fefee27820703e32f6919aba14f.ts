import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { LogChatMessages, ChatMessage } from 'src/app/models/models';

@Injectable({
  providedIn: 'root'
})
export class ChatMessagesService {
  private allRooms : LogChatMessages[] =[];

  private chatRoom = new BehaviorSubject<LogChatMessages[]>([]);
  public room = this.chatRoom.asObservable();

  private chatScrollDown = new BehaviorSubject<[string, boolean]>(["", false]);
  public reDown = this.chatScrollDown.asObservable();

  private validConnection = new BehaviorSubject<boolean>(true);
  public connection = this.validConnection.asObservable();

  constructor() { }

  public addNewGroup(groupName:string, msgs:ChatMessage[]){
    if(this.groupExists(groupName)){
      this.setConnection(false);
      return;
    }
    this.chatRoom.value.push({
      "groupName": groupName,
      "messages": msgs,
      "newMessages": 0
    });
  }

  public addMessage(groupName:string, msg:ChatMessage){
    if(!this.groupExists(groupName)){
      this.setConnection(false);
      return;
    }
    this.chatRoom.value.forEach(r=>{
      if(r.groupName == groupName){
        r.messages.push(msg);
        r.newMessages = r.newMessages+1;
      }
    });
    this.sendReDown(groupName);
  }

  public setConnection(value:boolean){
    return this.validConnection.next(value);
  }

  public groupExists(groupName){
    let exists = false;
    this.chatRoom.value.forEach(r=>{
      if(r.groupName == groupName) exists = true;
    });

    return exists;
  }

  private sendReDown(groupName:string){
    this.chatScrollDown.next([groupName, true]);
    setTimeout(_=> this.chatScrollDown.next([groupName, false]), 30);
  }
}