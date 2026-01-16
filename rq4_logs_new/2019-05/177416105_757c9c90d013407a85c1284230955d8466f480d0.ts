import { browser, by, element } from 'protractor';

export class ApplicationPage {

    // Método para obtener el título
    getTitleText() {
        const selector = 'body > app-root > div > app-trip-display > section > div.container > div > div > div > div.card-desc > p:nth-child(1)';
        return element(by.css(selector)).getText();
    }


    requestTrip () {
        const buttonSelector = 'body > app-root > div > app-trip-display > section > div.container > div > div > div > div.card-desc > button';
        element(by.css(buttonSelector)).click();
    }
}

