import {chromium, expect, Page, test} from "@playwright/test";
import {selectByAriaLabel} from "../support/playwright-utils";

test.describe('Dashboard Home Tests', () => {

  test.beforeEach(async ({page}) => {
    // Optional: Setzen Sie die Seite in einen bekannten Zustand vor jedem Test
    await page.goto('http://localhost:4200/');
    await loginAsBalu(page);
    await page.waitForURL('http://localhost:4200/dashboard');
  });

  test.afterEach(async ({ page }) => {
    // 1. Navigation zur Startseite oder einer definierten Seite
    await page.goto('http://localhost:4200/');

    // 2. Löschen von Cookies und lokalem Speicher
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await page.context().clearCookies();

    // 3. Optional: Neuladen der Seite
    await page.reload();
  });

  // TODO: Fix test
  // test('Check register base page works', async ({ page }) => {
  //   // Open App and Login
  //   await expect(page.locator('#settings')).toBeVisible();
  //   await page.locator('#settings').click();
  //   await expect(page.locator("#settings-container")).toBeVisible();
  //   await expect(page.locator("#navigate-to-base-button")).toBeVisible();
  //   await page.locator("#navigate-to-base-button").click();
  //   await page.locator('#register-base-button').click();
  //   await page.getByPlaceholder('sensor location...').click();
  //   await page.getByPlaceholder('sensor location...').fill('Test Location');
  //   await page.locator('#register-modal-register-button').click();
  // });
})


async function loginAsBalu(page: Page) {
  await expect(page.locator(selectByAriaLabel("aq-login-button"))).toBeVisible();
  await page.locator(selectByAriaLabel("aq-username-input")).fill("balu");
  await page.locator(selectByAriaLabel("aq-password-input")).fill("s3cret!");
  return page.locator(selectByAriaLabel("aq-login-button")).click();
}

async function loginAsMogli(page: Page) {
  await expect(page.locator(selectByAriaLabel("aq-login-button"))).toBeVisible();
  await page.locator(selectByAriaLabel("aq-username-input")).fill("mogli");
  await page.locator(selectByAriaLabel("aq-password-input")).fill("s3cret!");
  return page.locator(selectByAriaLabel("aq-login-button")).click();
}