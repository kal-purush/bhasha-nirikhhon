import { browser, by, element } from 'protractor';
import {until} from 'selenium-webdriver';
import ableToSwitchToFrame = until.ableToSwitchToFrame;

export class ViewerPage {
  navigateTo() {
    return browser.get('/summary');
  }

  isLoaded() {
    return element.all(by.css('h1[data-hook="em.anno.summary.annotation"]')).count().then((count => {
      return count > 0;
    }));
  }

  getNumberOfAnnotations() {
    return element.all(by.css('h1[data-hook="em.anno.summary.annotation"]')).count()
  }





}