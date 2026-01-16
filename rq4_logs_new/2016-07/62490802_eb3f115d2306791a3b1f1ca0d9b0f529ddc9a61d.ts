import {IStep}                from './istep';
import {inject, NewInstance,
  BindingEngine, Disposable}  from 'aurelia-framework';
import {ValidationRules}      from 'aurelia-validatejs';
import {ValidationController} from 'aurelia-validation';
import {Notification}         from '../shared/notification';
import {FS}                   from 'monterey-pal';

@inject(NewInstance.of(ValidationController), BindingEngine, Notification)
export class ProjectName {
  state;
  step: IStep;
  available: boolean;
  observer: Disposable;

  constructor(private validation: ValidationController,
              private bindingEngine: BindingEngine,
              private notification: Notification) {
  }

  async activate(model) {
    this.state = model.state;
    this.step = model.step;
    this.step.execute = () => this.execute();
    this.step.previous = () => this.previous();

    this.observer = this.bindingEngine.propertyObserver(this.state, 'name').subscribe(() => this.nameChanged());
  }

  attached() {
    ValidationRules
    .ensure('name').required()
    .on(this.state);

    this.checkFolderExistence();
  }

  nameChanged() {
    this.checkFolderExistence();
  }

  async checkFolderExistence() {
    if (!this.state.name) return;

    let path = FS.join(this.state.path, this.state.name);
    let folderExists = await FS.folderExists(path);
    this.available = !folderExists;
  }

  async execute() {
    let canContinue = false;

    if (this.validation.validate().length === 0) {
      if (this.available) {
        canContinue = true;
        this.observer.dispose();
      } else {
        this.notification.error('Please select a project folder that does not exist yet');
      }
    } else {
      this.notification.error('There are validation errors');
    }

    return {
      goToNextStep: canContinue
    };
  }

  detached() {
    this.observer.dispose();
  }

  async previous() {
    return {
      goToPreviousStep: true
    };
  }
}