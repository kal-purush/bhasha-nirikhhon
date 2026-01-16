import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { FormBuilder, FormGroup, Validators, AbstractControl } from '@angular/forms';

import { Message } from '../../models/message.interface';

@Component({
    selector: 'app-chat-room',
    styleUrls: ['chat-room.component.scss'],
    templateUrl: 'chat-room.component.html'
})

export class ChatRoomComponent implements OnInit {
    @Input()
    messages: Message;
    @Output()
    formSubmit = new EventEmitter;

    control: AbstractControl;
    chatForm: FormGroup;
    formInvalid = false;

    constructor(private fb: FormBuilder) {
        this.createForm();
     }

    ngOnInit() {
        this.control = this.chatForm.get('body');
     }

     createForm() {
        this.chatForm =  this.fb.group({
             body: [ '', Validators.required ]
        });
     }

     sendMsg(value, valid) {
         if (valid) {
           this.formInvalid = false;
           this.formSubmit.emit(value);
           this.control.reset();
         } else {
            this.formInvalid = true;
         }
     }
}