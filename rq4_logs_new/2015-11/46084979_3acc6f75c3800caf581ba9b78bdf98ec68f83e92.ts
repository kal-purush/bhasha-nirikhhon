import { Component, View, NgClass, Input, SimpleChange} from 'angular2/angular2';

import {default as zxcvbn} from 'zxcvbn';

@Component({
  selector: 'password-strength-bar'
})
@View({
  templateUrl: 'src/shared/password-strength-bar.html',
  directives: [NgClass]
})
export class PasswordStrengthBar {
  @Input() password: string;

  private passwordStrength: any;
  private passwordStrengthComment: string;
  private progressBarClassMap: any = {};

  constructor() {
    //this.calculatePasswordStrength();
  }

  private onChanges(changes: {[propName: string]: SimpleChange}): void {
    this.calculatePasswordStrength();
  }

  private calculatePasswordStrength(): void {
    this.passwordStrength = zxcvbn(this.password);

    switch(this.passwordStrength.score) {
      case 0:
        this.passwordStrengthComment = "Very Weak";
        this.progressBarClassMap = {'progress-bar-danger': true};
        break;
      case 1:
        this.passwordStrengthComment = "Weak";
        this.progressBarClassMap = {'progress-bar-warning': true};
        break;
      case 2:
        this.passwordStrengthComment = "Ok";
        this.progressBarClassMap = {'progress-bar-info': true};
        break;
      case 3:
        this.passwordStrengthComment = "Strong";
        this.progressBarClassMap = {'progress-bar-success': true};
        break;
      case 4:
        this.passwordStrengthComment = "Very Strong";
        this.progressBarClassMap = {'progress-bar-success': true};
        break;
    }
  }
}