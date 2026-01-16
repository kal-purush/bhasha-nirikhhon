import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://localhost:3000/democosmos/');
  await page.getByRole('button', { name: 'Settings' }).click();
  await page.getByLabel('URL', { exact: true }).click();
  await page.getByLabel('URL', { exact: true }).fill('');
  await page.getByLabel('portalName').click();
  await page.getByLabel('portalName').fill('');
  await page.getByLabel('clientid').click();
  await page.getByLabel('clientid').fill('');
  await page.getByLabel('clientsecret').click();
  await page.getByLabel('clientsecret').fill('');
  await page.getByLabel('Application name').click();
  await page.getByLabel('Application name').fill('');
  await page.getByRole('button', { name: 'Update' }).click();
  await page.getByRole('button', { name: 'Hide settings' }).click();
  await page.goto('https://localhost:3000/democosmos/');
  await page.getByPlaceholder('User name').click();
  await page.getByPlaceholder('User name').fill('');
  await page.getByPlaceholder('User name').press('Tab');
  await page.getByPlaceholder('Password').fill('');
  await page.getByPlaceholder('Password').press('Enter');

  await page.getByRole('button', { name: 'Search' }).click();
  await page.getByRole('button', { name: 'Demo App' }).click();

  // Expects page to have a heading with the name of Installation.
  await expect(page.getByRole('heading', { name: 'Home' })).toBeVisible();
});