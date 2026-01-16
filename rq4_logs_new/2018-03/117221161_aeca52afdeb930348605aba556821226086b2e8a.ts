import { browser, by, element } from 'protractor';
import {promise as wdpromise} from 'selenium-webdriver';
import {ElementFinder} from 'protractor/built/element';

export class DocumentRow {

  constructor(private document: ElementFinder) {}

  name() {
    return this.document.element(by.css('td[data-hook="dm-listview__document__name"')).getText();
  }

  view() {
    return this.document.element(by.css('a[data-hook="dm-listview__document__view')).click();
  }

  annotate() {
    return this.document.element(by.css('a[data-hook="dm-listview__document__annotate')).click();
  }
}

export class ListViewPage {
  navigateTo() {
    return browser.get('/list');
  }

  getTitleText() {
    return element(by.css('h1[data-hook="dm-listview__heading"]')).getText();
  }

  getDocument(row: number): DocumentRow {
    return new DocumentRow(element.all(by.css('tr[data-hook="dm-listview__document"')).get(row));
  }

  hasDocuments() {
   return element.all(by.css('tr[data-hook="dm-listview__document"')).count().then(count => {
     return count > 0;
   });
  }
}
