/**
 * Created by Lawrence on 3/16/16.
 */
import {Component} from 'angular2/core';

@Component({
    selector: 'my-input',
    template: `
    <h1>Your details please...</h1>
    <div>
        <label for="name">Your Name</label>
        <input type="text" id="name" [(ngModel)]="myself.name" (keyup)="onKeyUp()">
    </div>
    <div>
        <label for="age">Your Age</label>
        <input type="text" id="age" [(ngModel)]="myself.age" (keyup)="onKeyUp()">
    </div>
    <br>
    <div>Filled out: {{ isFilled ? 'Yes' : 'No' }}</div>
    <div>Valid: {{ isValid ? 'Yes' : 'No' }}</div>
    <br>
    <button [disabled]="!isValid">Submit</button>
    `
})

export class InputComponent {
    myself = {name: '', age: ''};
    isFilled = false;
    isValid = false;

    onKeyUp() {
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
}