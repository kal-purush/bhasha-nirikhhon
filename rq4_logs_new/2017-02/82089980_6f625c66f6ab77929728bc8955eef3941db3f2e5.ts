import {CommonModule} from "@angular/common";
import {NgModule} from "@angular/core";
import {FormsModule} from "@angular/forms";

import {MessagesComponent} from "./messages.component";
import {MessageListComponent} from "./message-list.component";
import {MessageComponent} from "./message.component";
import {MessageInputComponent} from "./message-input.component";
import {MessageService} from "./message.service";

@NgModule({
    declarations: [
        MessagesComponent,
        MessageListComponent,
        MessageComponent,
        MessageInputComponent
    ],
    imports: [
        CommonModule,
        FormsModule
    ],
    //It'll create one single instance of the MessageService.
    //Because the injection is hierarchical, this same instance will be shared.
    providers: [MessageService]
})
export class MessageModule {

}