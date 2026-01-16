import { test as setup, expect } from '@playwright/test';
import { elementSelectors } from '../selectors/elementSelectors';
import { STORAGE_STATE } from '../playwright.config';

setup('sign in', async ({ page }) => {
  await page.goto('/sign-in');

  const USERNAME = process.env.PW_USERNAME;
  const PASSWORD = process.env.PW_PASSWORD;

  // in case dotenv stops working
  if (USERNAME !== undefined) {
    await elementSelectors.usernameInput(page).fill(USERNAME);
  } else {
    console.error('USERNAME environment variable is not set or is undefined.');
  }

  if (PASSWORD !== undefined) {
    await elementSelectors.passwordInput(page).fill(PASSWORD);
  } else {
    console.error('USERNAME environment variable is not set or is undefined.');
  }

  await elementSelectors.signInButton(page).press('Enter');

  await expect(page.getByText('My Organizations', { exact: true })).toBeVisible();

  console.log("===== global setup sign in function =====")

  await page.context().storageState({ path: STORAGE_STATE });
});