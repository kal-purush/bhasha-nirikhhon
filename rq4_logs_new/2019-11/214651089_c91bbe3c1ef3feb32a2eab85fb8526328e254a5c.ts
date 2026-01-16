import {newE2EPage} from "@stencil/core/testing";

describe('cart-button', () => {
  it('renders', async () => {
    const page = await newE2EPage();

    await page.setContent('<rg-cart-button></rg-cart-button>');
    const element = await page.find('rg-cart-button');
    expect(element).toHaveClass('hydrated');
  });

  it('should render cart quantity', async function () {
    const page = await newE2EPage();

    await page.setContent('<rg-cart-button></rg-cart-button>');

    await page.$eval('rg-cart-button', (elm: any) => {
      elm.cartQuantity = 100;
    });

    await page.waitForChanges();

    const textElement = await page.find('rg-cart-button >>> .cart-button__text--active');
    expect(textElement).toEqualHtml('<span class="cart-button__text--active"><slot></slot> <span>(100)</span></span>');
  });
});