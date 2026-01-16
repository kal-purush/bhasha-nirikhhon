import {
  Component,
  Input
} from '@angular/core';

@Component({
  selector: 'note-card',
  styles: [`
    .note-card: {
      padding: 15px;
      border-radius: 2px;
      width: 100%;
      position: relative;
    }
    .title {
      font-size: 1.2em;
      font-weight: bold;
      text-align: left;
      color: rgba(0,0,0,0.8);
    }
    .value {
      text-align: left;
      font-size: 1.4em;
      font-weight: 200;
    }
    .icon {
      position: relative;
      color: black;
      border: 1px solid lightgrey;
      background-color: white;
      font-size: 30px;
      top: -10px;
      left: -10px;
      width: 40px;
      height: 40px;
      border-radius: 100%;
      cursor: pointer;
    }
    `],
    template: `
      <div class="note-card row shadow-1">
        <div class="icon">
          <i class="material-icons">check</i>
        </div>
        <div class="col-xs-12 title">{{ note.title }}</div>
        <div class="col-xs-12 value">{{ note.value }}</div>
      </div>
    `,
})

export class NoteCard {
  // This input component allows us to pass in note data
  @Input() note = {}; //  Take in a note
};