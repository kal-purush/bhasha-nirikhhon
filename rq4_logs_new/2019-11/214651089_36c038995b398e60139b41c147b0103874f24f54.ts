import { newE2EPage } from '@stencil/core/testing';

describe('rg-navigation', () => {
  it('renders', async () => {
    const page = await newE2EPage();

    await page.setContent(`<rg-navigation-mobile></rg-navigation-mobile>`);

    await page.$eval('rg-navigation-mobile', (elm: any) => {
      const dataSet = [
        {
          name: "smth",
          link: "localhost",
          children: [
            {
              name: "child",
              link: "localhost"
            }
          ],
        }
      ];
      elm.navigationFields = dataSet;
    });

    await page.waitForChanges();

    const element = await page.find('rg-navigation-mobile');
    expect(element).toHaveClass('hydrated');
  });

  it('displays mobile menu on click', async () => {
    const page = await newE2EPage();

    await page.setContent(`<rg-navigation-mobile></rg-navigation-mobile>`);

    await page.$eval('rg-navigation-mobile', (elm: any) => {
      const dataSet = [
        {
          name: "smth",
          link: "localhost",
          children: [
            {
              name: "child",
              link: "localhost"
            }
          ],
        }
      ];
      elm.navigationFields = dataSet;
    });

    await page.waitForChanges();

    const menuButton = await page.find('rg-navigation-mobile >>> .mobile-menu-toggle');
    await menuButton.click();

    const menu = await page.find('rg-navigation-mobile >>> .side-navigation');

    expect(menu).toHaveClass('side-navigation--opened');
  });
});