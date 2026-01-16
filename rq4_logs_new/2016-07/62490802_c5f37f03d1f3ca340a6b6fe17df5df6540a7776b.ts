import {inject, NewInstance}     from 'aurelia-framework';
import {DialogController}        from 'aurelia-dialog';
import {ApplicationState}        from './application-state';
import {ValidationRules}         from 'aurelia-validatejs';
import {ValidationController}    from 'aurelia-validation';

@inject(NewInstance.of(ValidationController), DialogController, ApplicationState)
export class ManageEndpoints {

  tempState = {};

  constructor(private validation: ValidationController,
              private dialog: DialogController,
              private state: ApplicationState) {}
  attached() {  

    Object.assign(this.tempState, this.state.endpoints);

    ValidationRules
    .ensure('montereyRegistry').required().url()
    .ensure('npmRegistry').required().url()
    .ensure('githubApi').required().url()
    .ensure('github').required().url()
    .on(this.tempState);
  }

  submit() {
    if (this.validation.validate().length > 0) {
      alert('There are validation errors');
      return;
    }

    Object.assign(this.state.endpoints, this.tempState);

    this.state._save();

    this.dialog.ok();
  }
}
