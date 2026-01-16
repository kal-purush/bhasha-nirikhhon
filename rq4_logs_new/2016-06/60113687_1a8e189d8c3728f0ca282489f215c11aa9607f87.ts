import {Component} from '@angular/core';

@Component({
  selector: 'twoway-bind',
  template: `
<hr>
<br>
<br>
<h3>Two-Way Binding</h3>
<br>
<p>data binding both ways.</p>

              {{twowayTest()}}

              <input type="text" [(ngModel)]="friendName" placeholder="enter friend name..">
              <p>Your friends name: {{friendName}}</p>


  `,


})

export class TwowayBindingComponent {

  nickName = '';
  values = '';
  friendName = 'Nikki';

  twowayTest() {
    return 0 === 0;
  }

}
