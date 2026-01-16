import { AppPage } from './../app.po';
import { browser } from 'protractor/built';
import { by, element } from 'protractor';

describe('online-store App', () => {
  let page: AppPage;


  beforeEach(() => {
    page = new AppPage();
  });

  it('should log in', async () => {
    await browser.get('/');
    await browser.driver.findElement(by.id('nav-login'));
    element(by.id('nav-login')).click();
    await browser.driver.findElement(by.id('login-email'));
    element(by.id('login-email')).sendKeys('test@test.bg');
    element(by.id('login-pass')).sendKeys('test123');  
    await element(by.id('login-submit')).click();
    await browser.waitForAngular();
    expect(browser.driver.getCurrentUrl()).toMatch('/');


    // await browser.get('/');
    // await browser.driver.findElement(by.id('logged-user'));

  });

//   it('should change password', async () => {
//     await browser.get('/users/details') // TODO use the dropdown menu, activate Guard for thir route
//     await browser.driver.findElement(by.id('cp-oldPass'));
//     element(by.id('cp-oldPass')).sendKeys('test123');
//     element(by.id('cp-newPass')).sendKeys('test12345');
//     element(by.id('cp-confirmPass')).sendKeys('test12345');
//     element(by.id('cp-submit')).click();
//   });

//   it('should show orders', async () => {
//     await browser.get('/users/viewOrders')
//     await browser.driver.findElements(by.css('viewOrders'));
//     const elementh1 = element(by.css('viewOrders'));  
//     let text = elementh1.getText();  
//     expect(text).toBe(`Orders:`);
//   });

});