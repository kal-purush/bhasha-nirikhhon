
import {setupBrowserHooks, getBrowserState} from './utils';

describe('App test', function () {
  setupBrowserHooks();
  it('signs in a registered user', async function () {
    const {page} = getBrowserState();
    await page.locator('#email').fill('user1@test.com');
    await page.locator('#password').fill('aB3DfG5hI7jK9mN1pQrStU3');
    await page.locator('#btn-submit').click();

    await page.waitForNavigation();
    const text = await page.locator('#btn-save').wait();
    expect(text).not.toBeNull();
  });
});