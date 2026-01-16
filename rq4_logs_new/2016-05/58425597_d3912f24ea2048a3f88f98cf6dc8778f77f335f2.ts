export class BasePage {

  goToHome() {
    return browser.get('/');
  }

  currentTitle():webdriver.promise.Promise<string> {
    return element(by.css('title')).getInnerHtml();
  }

  getCurrentUrl() {
    return browser.getCurrentUrl();
  }

  wait():Promise<any> {
    return new Promise<any>((resolve) => {
      this.verifySpinner(resolve);
    });
  }

  fill(input, value:string) {
    return value.split('').forEach((c) => input.sendKeys(c));
  }

  private verifySpinner(resolve):webdriver.promise.Promise<any> {
    return element(by.css('ion-spinner')).isPresent()
      .then(
        (present) => {
          if (present) {
            setTimeout(() => this.verifySpinner(resolve), 100);
          } else {
            resolve();
          }
        }
      );
  }
}