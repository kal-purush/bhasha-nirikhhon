import { MsgBetweenService } from './../msg-between.service';
import { Component, EventEmitter, Output } from '@angular/core';

@Component({
  selector: 'app-msg-child4',
  template: `
  msg-between working, child 4: <b>SENDINGS</b> through <b>@Output in parent</b>
  <br>
  This is only for directly passing from child to parent, as @Input component can only be used in parent.
  <br>
  <div class="input-group input-group-lg">
    <input type="text" class="form-control margin10px comment" 
      placeholder="Type something..." 
      aria-label="Sizing example input" 
      aria-describedby="inputGroup-sizing-lg" 
      #postMessage>
    <button type="button" class="btn btn-success margin10px" 
      (click)="addPostMessage(postMessage)">Send
    </button>
  </div>
  `
})
export class MsgChild4Component {
  @Output() postData = new EventEmitter();

  constructor(private msgBetweenService: MsgBetweenService) {
  }
  
  addPostMessage(postMessage: HTMLInputElement) {
    // Using EventEmitter in @Output
    this.postData.emit(postMessage.value);

    // Using EventEmitter in service
    this.msgBetweenService.chatMessageAdded.emit(postMessage.value)
    postMessage.value = '';
  }

}