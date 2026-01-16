import { browser, by, element } from 'protractor';

export class ConnexionPage {

  navigateTo(path) {
    return browser.get(path);
  }

  url() {
    return browser.getCurrentUrl();
  }

  getParagraphText() {
    return element(by.css('app-root h1')).getText();
  }

  fillAndSendFormConnection(login,passwd) {
    var form =  browser.findElement(by.id('login'));
    form.findElement(by.css('input[ng-model=mail]')).sendKeys(login)
    .then(function() {
      form.findElement(by.css('input[ng-model=mdp]')).sendKeys(passwd);
    }).then(function() {
      form.findElement(by.css('input[type=submit]')).click();
    });
  }
  fillAndSendFormCreateProfile(login, passwd) {
    var form = browser.findElement(by.id('signup'));
    form.findElement(by.css('input[ng-model=new_mail]')).sendKeys(login)
    .then(function() {
      form.findElement(by.css('input[ng-model=new_mdp]')).sendKeys(passwd);
    }).then(function() {
      form.findElement(by.css('input[ng-model=new_mdp_conf]')).sendKeys(passwd);
    }).then(function() {
      form.findElement(by.css('input[type=submit]')).click();
  });
  }

  errorMessageExist() {
    //return form.findElement(by.css()).getText();
  }
}