import {inject, NewInstance}  from 'aurelia-framework';
import {ApplicationState}     from '../../shared/application-state';
import {withModal}            from '../../shared/decorators';
import {GithubCreds}          from '../../shared/github-creds';
import {SESSION}              from 'monterey-pal';
import {ValidationRules}      from 'aurelia-validatejs';
import {ValidationController} from 'aurelia-validation';

@inject(ApplicationState, NewInstance.of(ValidationController))
export class Screen {
  constructor(private state: ApplicationState,
              private validation: ValidationController) {
  }

  attached() {
    ValidationRules
    .ensure('checkForUpdatesOnStartup')
    .on(this.state);
  }

  async save() {
    if (this.validation.validate().length > 0) {
      alert('There are validation errors');
      return;
    }

    await this.state._save();
    alert('Changes saved');
  }

  clearGithub() {
    this.state.gitAuthorization = null;
  }

  @withModal(GithubCreds)
  async configureGithub() {
  }
}