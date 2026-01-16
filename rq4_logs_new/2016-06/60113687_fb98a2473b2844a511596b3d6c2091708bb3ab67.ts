import {Component, EventEmitter} from '@angular/core';

@Component({
  selector: 'my-confirm',
  template: `

        <h2>You submitted this info</h2>
          <h4>Please confirm it!</h4>

            <p>Your name is <span class="highlight">{{myself.name}}</span> 
                and you're <span class="highlight">{{myself.age}}</span>
            </p>

          <div>
            <label for="name">Name</label>
              <input type="text" id="name" [(ngModel)]="myself.name" (keyup)="onKeyUp()"> 
          </div>

          <div>
            <label for="age">Age</label>
              <input type="text" id="age" [(ngModel)]="myself.age" (keyup)="onKeyUp()">
          </div>          
           <br>
          <div>Filled Out: {{isFilled ? 'Yes' : 'No'}}</div>
          <div>Valid: {{isValid ? 'Yes' : 'No'}}</div>
          <br>
          <button [disabled]="!isValid" (click)="onConfirm()">confirm</button>          


  `,
  inputs: ['myself'],
  outputs['confirmCustom']
})

export class ConfirmComponent {
    
  myself = { name: '', age: '' };
  isFilled = false;
  isValid = false;
  confirmCustom = new EventEmitter<{ name: string, age: string }>();

  onKeyUp(){
  if (this.myself.name != '' && this.myself.age != '') {
    this.isFilled = true;
  } else {
    this.isFilled = false;
  }
  if (this.myself.name != '' && /^\d+$/.test(this.myself.age)) {
    this.isValid = true;

  } else {
    this.isValid = false;
  }
}

onConfirm() {
  this.confirmCustom.emit(this.myself);
}

}