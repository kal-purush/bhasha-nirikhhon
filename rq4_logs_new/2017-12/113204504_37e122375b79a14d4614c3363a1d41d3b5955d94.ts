import {
  Component,
  Input,
  OnInit
} from '@angular/core';
import { FormGroup } from '@angular/forms';

@Component({
  selector: 'app-rule-goal-control',
  templateUrl: './rule-goal-control.component.html',
  styleUrls: ['./rule-goal-control.component.scss']
})
export class RuleGoalControlComponent implements OnInit {

  @Input('group') public form: FormGroup;
  @Input('index') public index: number;

  public validatorMessages = {
    goalControl: {
      required: 'Vui lòng nhập loại bàn thắng'
    }
  };
  public formErrors = {
    goalControl: '',
  };

  constructor() {
  }

  public ngOnInit() {
    this.form.valueChanges
      .subscribe(() => this.validatorForm());
  }

  public validatorForm(submitted?: boolean) {
    if (!this.form) {
      return;
    }
    const form = this.form;
    for (const field in this.formErrors) {
      if (this.formErrors.hasOwnProperty(field)) {
        // clear previous error message (if any)
        this.formErrors[field] = '';
        const control = form.get(field);
        if (submitted) {
          control.markAsTouched();
          control.markAsDirty();
        }
        // self.hasError = false;
        if (control && control.dirty && !control.valid) {
          const messages = this.validatorMessages[field];
          for (const key in control.errors) {
            if (control.errors.hasOwnProperty(key)) {
              this.formErrors[field] += messages[key] + ' ';
              break;
            }
          }
        }
      }
    }
  }



}