import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Session} from "./session.service";

@Component({
  selector: 'session',
  styleUrls: ["./session.component.css"],
  template: `
    <grid-layout columns="auto, *" rows="auto, auto, auto" class="container" (tap)="selected.next()">
      <image rowSpan="3"
        [src]="session.isFav ? 'res://fav_1' : 'res://fav_0'"
        [class.fav]="session.isFav"></image>
      
      <Label col="1" row="0" [text]="(session.start | date:'HHmm') + ' - ' + (session.end | date:'HHmm')" class="time"></Label>
      <Label col="1" row="1" [text]="session.title" textWrap="true" class="title"></Label>
      <Label col="1" row="2" [text]="session.speaker" class="speaker"></Label>              
    </grid-layout>
  `
})
export class SessionComponent {
  @Input() session: Session;
  @Output() selected: EventEmitter<any> = new EventEmitter();
}