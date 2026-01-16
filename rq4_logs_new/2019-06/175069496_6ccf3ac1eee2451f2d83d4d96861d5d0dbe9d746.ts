import { Component, OnInit, Input } from '@angular/core';
import { ChatModel } from 'src/app/models/models';
import { ChatService } from 'src/app/services/restServices/chat.service';
import { AuthenticationService } from 'src/app/services/restServices/authentication.service';
import { SessionService } from 'src/app/services/userServices/session.service';
import { FormGroup, FormControl, Validators } from '@angular/forms';


@Component({
  selector: 'app-chat-messages',
  templateUrl: './chat-messages.component.html',
  styles: []
})
export class ChatMessagesComponent implements OnInit{

  @Input() groupName:string ="";
  public messages:ChatModel[] = [];
  public sendChatMessageForm:FormGroup;

  constructor(private _chatS:ChatService, private _authS:AuthenticationService, private sessionS:SessionService) { 
    this.initializeForm();
  }

  ngOnInit(){
    if(this._authS.IsAuthenticated()){
      this._chatS.logChat(this.groupName);
      this.getMessages();
    }
  }

  public isValid(){
    return this._chatS.isValid();
  }

  private getMessages(){
    this._chatS.getConnection().on(this.groupName, (message:ChatModel)=>{
      this.messages.push(message);
      setTimeout(_=>this.scrollDown(), 10);
    });
  }

  private initializeForm(){
    this.sendChatMessageForm = new FormGroup({
      'message': new FormControl(
        '',
        [
          Validators.required,
          Validators.minLength(1),
          Validators.maxLength(150)
        ]
      )
    })
  }

  public send(){
    if(this.sendChatMessageForm.valid){
      this._chatS.broadcastChartData({
        "Group": this.groupName,
        "message": this.sendChatMessageForm.controls["message"].value,
        "user": ""
      });
      
      this.sendChatMessageForm.reset({
        "message" : ""
      });
    }
  }

  private scrollDown(){
    let div = (document.querySelector("#chatScroll") as HTMLElement);
    div.scrollTop = div.scrollHeight;
  }

}