
import { $, ElementFinder, promise } from 'protractor';
export class AddressStepPage {
  private button: ElementFinder;
  constructor() {
    this.button = $('#center_column > form > p > button > span');
  }
  public addressStep(): promise.Promise<void> {
    return this.button.click();
  }
}