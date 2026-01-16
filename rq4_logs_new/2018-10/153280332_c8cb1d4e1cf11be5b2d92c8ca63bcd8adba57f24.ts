import { $, browser } from 'protractor';

describe('Buy a t-shirt', () => {
  beforeEach(() => {
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 120000;
  });

  it('then a t-shirt should be bought', async () => {
    await browser.get('http://automationpractice.com/');
    await (browser.sleep(10000));
    await $('#block_top_menu > ul > li:nth-child(3) > a').click();
    await (browser.sleep(3000));
    await browser.actions().mouseMove(
      $('#center_column > ul.product_list > li:nth-child(1) > div.product-container')
    ).perform();
    await $('#center_column a.button.ajax_add_to_cart_button.btn.btn-default').click();
    await (browser.sleep(3000));
    await $('[style*="display: block;"] .button-container > a').click();
    await (browser.sleep(3000));
    await $('.cart_navigation span').click();
    await (browser.sleep(3000));

    await $('#email').sendKeys('aperdomobo@gmail.com');
    await $('#passwd').sendKeys('WorkshopProtractor');
    await $('#SubmitLogin > span').click();
    await (browser.sleep(3000));

    await $('#center_column > form > p > button > span').click();
    await (browser.sleep(3000));

    await $('#cgv').click();
    await (browser.sleep(3000));

    await $('#form > p > button > span').click();
    await (browser.sleep(3000));
    await $('#HOOK_PAYMENT > div:nth-child(1) > div > p > a').click();
    await (browser.sleep(3000));
    await $('#cart_navigation > button > span').click();
    await (browser.sleep(3000));

    await expect($('#center_column > div > p > strong').getText())
      .toBe('Your order on My Store is complete.');
  });
});