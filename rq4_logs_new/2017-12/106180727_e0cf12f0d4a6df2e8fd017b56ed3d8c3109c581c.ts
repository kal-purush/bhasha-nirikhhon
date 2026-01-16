import { browser, by, element } from 'protractor';
import { getPath } from './getpath';

describe('User update functionality', () => {

    beforeEach(() => {
        browser.get('');
    });

    it('should fail to update for empty credentials and write error messages', () => {
        element(by.css('input[type="text"]')).sendKeys('Nagy Gábor');
        element(by.css('input[type="password"]')).sendKeys('gaboo');
        element(by.buttonText('Bejelentkezés')).click();
        browser.sleep(3000);
        element(by.buttonText('Adatlap')).click();
        element(by.buttonText('Profil módosítása')).click();
        element(by.buttonText('Változtat')).click();
        browser.sleep(1000);
        expect(element(by.cssContainingText('ng-star-inserted','Hiba: Új jelszó vagy felhasználónév megadása kötelező!')).isDisplayed).toEqual(true);
    });
});