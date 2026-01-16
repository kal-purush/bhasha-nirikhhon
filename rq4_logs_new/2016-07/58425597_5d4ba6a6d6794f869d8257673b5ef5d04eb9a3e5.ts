import {BasePage} from './base.po';

export class SignUpPage extends BasePage {

  private form;
  private nameInput;
  private emailInput;
  private passwordInput;
  private passwordConfirmationInput;
  private submitButton;
  private cancelButton;

  constructor() {
    super();
    this.form = element(by.id('sign-up-form'));
    this.nameInput = element(by.css('.user-name'));
    this.emailInput = this.form.element(by.css('.user-email'));
    this.passwordInput = this.form.element(by.css('.user-password'));
    this.passwordConfirmationInput = this.form.element(by.css('.user-password-confirmation'));
    this.submitButton = this.form.element(by.id('button_ok'));
    this.cancelButton = this.form.element(by.id('button_cancel'));
  }

  getGreetings():webdriver.promise.Promise<string> {
    return element(by.css('equalizei-front-app h1')).getText();
  }

  setName(value: string) {
    return super.fill(this.nameInput, value);
  }

  setEmail(value: string) {
    return super.fill(this.emailInput, value);
  }

  setPassword(value: string) {
    return super.fill(this.passwordInput, value);
  }

  setPasswordConfirmation(value: string) {
    return super.fill(this.passwordConfirmationInput, value);
  }

  submit() {
    return this.submitButton.click();
  }

  cancel() {
    return this.cancelButton.click();
  }

  getAllErrorMessages() {
    return element.all(by.css('.error-group'));
  }

  hasErrorMessages() {
    return this.getAllErrorMessages().count().then(value => {
      return value > 0;
    });
  }

  formIsValid() {
    return this.getAllErrorMessages().count().then(value => {
      return value === 0;
    });
  }
}