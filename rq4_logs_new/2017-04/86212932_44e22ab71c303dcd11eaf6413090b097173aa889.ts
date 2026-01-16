// Because this file references protractor, you'll need to have it as a project dependency
import {ElementFinder, browser, element, by, promise} from 'protractor';
import Promise = promise.Promise;

import {TEST_URL} from "./test.config";

export class SampleApplicationPage {
    // nameInput = element(by.model('yourName'));
    image = element(by.tagName('img'));
    title = element(by.tagName('h5'));
    message = element(by.tagName('h2'));

    get() {
        browser.get(TEST_URL);
    }

    getImage(): ElementFinder {
        return this.image;
    }

    getImageSrc(): Promise<string> {
        return this.getImage().getAttribute('src');
    }

    getTitle(): Promise<string> {
        return this.title.getText();
    }

    getMessage(): Promise<string> {
        return this.message.getText();
    }
}