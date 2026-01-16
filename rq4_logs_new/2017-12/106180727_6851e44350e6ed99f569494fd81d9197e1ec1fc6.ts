import { browser, by, element } from 'protractor';
import { getPath } from './getpath';

describe('Login functionality', () => {

    beforeEach(() => {
        browser.get('');
    });

    it('should fail to login for empty credentials and write error message', () => {
        element(by.buttonText('Bejelentkezés')).click();
        expect(element(by.id('mat-error-0')).isDisplayed()).toBe(true);
        expect(element(by.id('mat-error-1')).isDisplayed()).toBe(true);
        expect(getPath()).toEqual('');
    });


    it('should fail to login for bad username and write error message', () => {
        element(by.css('input[type="text"]')).sendKeys('asdopiasidh');
        element(by.css('input[type="password"]')).sendKeys('adsasdasd');
        element(by.buttonText('Bejelentkezés')).click();
        expect(getPath()).toEqual('');
    });

    it('should fail to login for bad password and write error message', () => {
        element(by.css('input[type="text"]')).sendKeys('admin');
        element(by.css('input[type="password"]')).sendKeys('adsasdasd');
        element(by.buttonText('Bejelentkezés')).click();
        expect(getPath()).toEqual('');
    });

    it('should log in for valid credentials', () => {
        element(by.css('input[type="text"]')).sendKeys('admin');
        element(by.css('input[type="password"]')).sendKeys('admin');
        element(by.buttonText('Bejelentkezés')).click();
        expect(getPath()).toEqual('/ads');
    });
});