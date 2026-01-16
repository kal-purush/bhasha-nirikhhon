/// <reference path="./typings/angular-protractor/angular-protractor.d.ts" />
/// <reference path="./typings/jasmine/jasmine.d.ts" />
/// <reference path="./typings/requirejs/require.d.ts" />

export class LoginPage {

  private form = element(by.css('form'));
  private inputEmail = element(by.model('c.email'));
  private inputPassword = element(by.model('c.password'));
  private buttonLogin = element(by.buttonText('ログイン'));

  public open(): void {
    browser.get('http://localhost:9010/');
  }

  public getButtonLogin(): protractor.ElementFinder {
    return this.buttonLogin;
  }

  public getDisabledLoginButton(): protractor.ElementFinder {
    return element(by.css('button[disabled="disabled"]'));
  }

  public typeEmail(email: string, clearField: boolean): protractor.promise.Promise<protractor.ElementFinder> {
    var deferred = protractor.promise.defer();

    if (clearField) {
      this.inputEmail.clear().then(() => {
        this.setEmailText(email, deferred);
      });
    }
    else {
      this.setEmailText(email, deferred);
    }
    return deferred.promise;
  }

  public typePassword(password: string, clearField: boolean): protractor.promise.Promise<protractor.ElementFinder> {
    var deferred = protractor.promise.defer();
    if (clearField) {
      this.inputPassword.clear().then(() => {
        this.setPasswordText(password, deferred);
      });
    }
    else {
      this.setPasswordText(password, deferred);
    }

    return deferred.promise;
  }

  public getErrorMessage(): protractor.promise.Promise<string>{
      return element(by.css('span[ng-bind-html="message.content"]')).getText();
  }

  private setEmailText(email: string, deferred: protractor.promise.Deferred<protractor.ElementFinder>): void {
    this.inputEmail.sendKeys(email).then(() => {
      deferred.fulfill(this.inputEmail);
    });
  }
  private setPasswordText(password: string, deferred: protractor.promise.Deferred<protractor.ElementFinder>): void {
    this.inputPassword.sendKeys(password).then(() => {
      deferred.fulfill(this.inputPassword);
    });
  }
}

var loginPage = new LoginPage();
var EC = protractor.ExpectedConditions;

describe("Login", function() {

  loginPage.open();

  it ('login button should be disabled', function() {
    browser.wait(EC.presenceOf(loginPage.getDisabledLoginButton()), 1000);
  });

  it ('login button should be disabled with only an email', function() {
    loginPage.typeEmail('bizreach@sutead.com', false).then(() => {
      expect(loginPage.getButtonLogin().isEnabled()).toBe(false);
    });
  });

  it ('login button should be enabled now', function() {
    loginPage.typePassword('fakepassword', false).then((input) => {
      expect(loginPage.getButtonLogin().isEnabled()).toBe(true);
    });
  });

  it ('error message due to wrong login credentials', function() {
    loginPage.getButtonLogin().click().then(function() {
      loginPage.getErrorMessage().then((text) => {
        expect(text).toBe("Emailアドレスまたはパスワードが誤っています。");
      });
    });
  });

  it ('login button disabled due to invalid email structure', function() {
    loginPage.typeEmail('bizreach', true).then(() => {
      expect(loginPage.getButtonLogin().isEnabled()).toBe(false);
    });
  });

  it ('login button enabled again', function() {
    loginPage.typeEmail('bizreach@sutead.com', true);
    loginPage.typePassword('password', true).then(() => {
      expect(loginPage.getButtonLogin().isEnabled()).toBe(true);
    });
  });

  it ('login success', function() {
    loginPage.getButtonLogin().click();

    // TODO how to remove this sleep?!
    browser.sleep(2000);

    browser.getLocationAbsUrl().then((url) => {
      expect(url).toBe("/");
    });
  });

});