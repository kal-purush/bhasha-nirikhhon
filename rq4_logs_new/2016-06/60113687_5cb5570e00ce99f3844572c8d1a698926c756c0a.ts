import {Component, EventEmitter} from '@angular/core';

@Component({
  selector: 'my-input',
  template: `
        
        <h2>Your Info, please..</h2>

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
          <button [disabled]="!isValid" (click)="onSubmit()">submit</button>          

  `,
  outputs: ['submittedCustom'];

})

export class InputComponent {

  myself = {
    name: '',
    age: ''
  };
  //check form
  isFilled = false;
  isValid = false;
  //button custom event..
  submittedCustom = new EventEmitter<{ name: string, age: string }>();

  onKeyUp(){
    if(this.myself.name != '' && this.myself.age != ''){
       this.isFilled = true;
    } else {
      this.isFilled = false;
    }
    if(this.myself.name != '' && /^\d+$/.test(this.myself.age)){
       this.isValid = true;

    } else {
      this.isValid = false;
    }
  }

  onSubmit(){
    this.submitted.emit(this.myself);
  }


}