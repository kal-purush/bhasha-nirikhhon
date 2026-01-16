import 'dotenv/config';
import { test, expect } from '@playwright/test';
import { elementSelectors } from '../selectors/elementSelectors';

test('has title', async ({ page }) => {
  await page.goto("https://staging.getdirect.io/sign-in");

  await expect(page).toHaveTitle("Direct™ Property Management Software | Hotels and Vacation Rentals");
});

test('can authenticate', async ({ page }) => {
  await page.goto("https://staging.getdirect.io/sign-in");
  
  const USERNAME = process.env.USERNAME;
  console.log("$USERNAME");
  const PASSWORD = process.env.PASSWORD;
  console.log("$PASSWORD");
  
  if (USERNAME !== undefined) {
    await elementSelectors.usernameInput(page).fill(USERNAME);
  } else {
    console.error('USERNAME environment variable is not set or is undefined.');
  }

  if (PASSWORD !== undefined) {
    await elementSelectors.usernameInput(page).fill(PASSWORD);
  } else {
    console.error('USERNAME environment variable is not set or is undefined.');
  }

  await elementSelectors.signInButton(page).press('Enter');

  // await page.waitForTimeout(8000);  
});