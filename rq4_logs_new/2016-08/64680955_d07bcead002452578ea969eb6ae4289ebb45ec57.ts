import { Component } from '@angular/core';

@Component({
  selector: 'notes-container',
  styles: [`
    .note-card {
      padding: 15px;
      border-radius: 2px;
      width: 100%;
      position: relative;
    }
    .title {
      font-size: 1.2rem;
      font-weight: bold;
      text-align: left;
      color: rgba(0,0,0,0.8);
    }
    .value {
      text-align: left;
      font-size: 1.4rem;
      font-weight: 200;
    }
    .icon {
      position: absolute;
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
    <div class='row center-xs notes'>
      <div class='col-xs-6 creator'>
      note creator here
      </div>
      <div class='notes col-xs-8'>
        <div class='row between-xs'>
          note card here
        </div>
      </div>
    </div>
  `
})
export class Notes {};