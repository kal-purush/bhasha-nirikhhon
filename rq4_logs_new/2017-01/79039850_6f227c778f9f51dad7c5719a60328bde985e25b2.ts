import { Component } from '@angular/core';

@Component({
  selector: 'notes-container',
  template: `
    <div class="row center-xs notes">
      <div class="col-xs-6 creator"> Create note her</div>
      <div class="notes col-xs-8">
        <div class="row between-xs">
          <note-card
            class="col-xs-4"
            [note]="sample"
          >
          </note-card>
        </div>
      </div>
  `
})

export class NotesContainer {
  sample = {title: 'this is a note', value: 'eat some food'};
};