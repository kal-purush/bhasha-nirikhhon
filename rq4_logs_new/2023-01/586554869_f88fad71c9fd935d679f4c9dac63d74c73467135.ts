import { test } from '@playwright/test';
import { checkAccessibility, login } from './utils';

test.describe('Change password', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('should change the password', async ({ page }) => {
    await page.click('a:has-text("Admin")');
    await page.click('text=Changer de mot de passe');
    await checkAccessibility(page, 'Change password page should be accessible');
    await page.fill('text=Mot de passe actuel', 'incorrect');
    await page.fill('text=Nouveau mot de passe', 'password'); // same on purpose, to not disturb other tests
    await page.click('text=Changer le mot de passe');

    await test.expect(page.locator('text=Le changement de mot de passe a échoué')).toBeVisible();
    await page.fill('text=Mot de passe actuel', 'password');
    await page.click('text=Changer le mot de passe');
    await test.expect(page.locator('h1')).toHaveText('La Maison Solidaire');
  });
});