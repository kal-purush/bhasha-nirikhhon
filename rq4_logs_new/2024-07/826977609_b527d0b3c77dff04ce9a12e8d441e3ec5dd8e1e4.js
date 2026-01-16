const { test, expect } = require('@playwright/test');

const settings = {
  screenshot: {
    type: 'jpeg',
    quality: 70,
    fullPage: true,
  },
  viewport: {
    width: 1280,
    height: 400,
  },
};

test.describe('Product cards template', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/1-percent.html');
    await page.setViewportSize(settings.viewport);
  });

  test('Product cards with a viewport width of 1200px', async ({ page }) => {
    const template = await page
      .locator('html')
      .screenshot(settings.screenshot);

    expect(template).toMatchSnapshot();
  });

  test('Product cards with a viewport width of 850px', async ({ page }) => {
    await page.setViewportSize({ width: 850, height: 400 });

    const template = await page
      .locator('html')
      .screenshot(settings.screenshot);

    expect(template).toMatchSnapshot();
  });

  test('Product cards with a viewport width of 615px', async ({ page }) => {
    await page.setViewportSize({ width: 615, height: 400 });

    const template = await page
      .locator('html')
      .screenshot(settings.screenshot);

    expect(template).toMatchSnapshot();
  });
});