import {inject, NewInstance}     from 'aurelia-framework';
import {DialogController}        from 'aurelia-dialog';
import {SESSION}                 from 'monterey-pal';
import {ValidationRules}         from 'aurelia-validatejs';
import {ValidationController}    from 'aurelia-validation';

@inject(NewInstance.of(ValidationController), DialogController)
export class GithubCreds {
  username: string;
  password: string;

  constructor(private validation: ValidationController, 
              private dialog: DialogController) {}

  activate(model) {
    
  }

  attached() {
    ValidationRules
    .ensure('username').required()
    .ensure('password').required()
    .on(this);
  }

  submit() {
    if (this.validation.validate().length > 0) {
      alert('There are validation errors');
      return;
    }
    
    SESSION.set('gitAuthorization', btoa(`${this.username}:${this.password}`));

    this.dialog.ok();
  }
}
