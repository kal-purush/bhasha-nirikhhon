import { ProtractorTestProjectPage } from './app.po';
import { browser, element, by } from 'protractor';

describe('protractor-test-project App', () => {
  

  beforeEach(() => {
    
  });

  it('waitForAngularEnabled test ', () => {
    
    // here in my project i use to go to an sso page (openam -> no angular app) for authentication
    browser.waitForAngularEnabled(false);

    browser.get('https://www.google.fr');
    browser.wait(() => element(by.id('lst-ib')).isPresent(), 10000, 'too long');
    element(by.id('lst-ib')).sendKeys("protractor issue waitForAngularEnabled");

    // i return to my app
    browser.get('/');

    browser.waitForAngularEnabled(true);
    let titleElement = element(by.id("title"));
    expect(titleElement.isPresent())

  });
});