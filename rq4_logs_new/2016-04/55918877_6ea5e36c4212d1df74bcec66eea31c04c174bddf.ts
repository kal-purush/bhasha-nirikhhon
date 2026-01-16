import {Component} from 'angular2/core';

@Component({
  selector: 'control-errors',
  template: `
    <h3>Control errors</h3>
    <p>
      All errors exist in <code>email.errros</code> and
      <code>password.errros</code>
    </p>
  `
})
export class ControlErrorsComponent { }