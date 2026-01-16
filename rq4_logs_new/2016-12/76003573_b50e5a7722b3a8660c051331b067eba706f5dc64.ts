import { BrowserModule } from '@angular/platform-browser';
import { PurchasePage } from './purchase.po';
import { browser, element, by } from 'protractor';

const name = 'Duncan Hunter';
const expiry = '1111';
const cardNo = '1111111111111111';
const nameSelector = '.name input';
const cardNoSelector = '.card-no input';
const expirySelector = '.expiry input';

beforeEach(async () => {
  await PurchasePage.navigateTo();
});

describe('firebootcamp-angular-testing App', () => {

  it('should have an active purchase button with valid form', async () => {

    PurchasePage.getInput(expirySelector).sendKeys(expiry);
    PurchasePage.getInput(cardNoSelector).sendKeys(cardNo);
    PurchasePage.getInput(nameSelector).sendKeys(name);
    // browser.sleep(2000);
    // const elementText = await PurchasePage.getInputText(nameSelector);
    const purchaseButtonAttr = PurchasePage.getPurchaseButtonDisabledAttr();

    expect(await purchaseButtonAttr).toEqual('false');

  });
});