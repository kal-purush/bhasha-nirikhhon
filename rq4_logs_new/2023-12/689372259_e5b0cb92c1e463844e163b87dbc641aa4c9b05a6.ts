import { test, expect } from '@playwright/test';

test('Pokemon page can be rendered when selectedGame is generic', async ({ page }) => {
	await page.context().clearCookies();
	await page
		.context()
		.addCookies([{ name: 'selectedGame', value: 'generic', domain: 'localhost', path: '/' }]);
	await page.goto('/pokemon/25', { waitUntil: 'networkidle' });

	const locator = page.locator('#pokemonName');
	await expect(locator).toHaveText('Pikachu');
});

test('When Pokemon is not in game, will display info but state that it is not in this game', async ({
	page
}) => {
	await page.context().clearCookies();
	await page
		.context()
		.addCookies([{ name: 'selectedGame', value: 'black-white', domain: 'localhost', path: '/' }]);
	await page.goto('/pokemon/957', { waitUntil: 'networkidle' }); // Tinkaton

	const nameLocator = page.locator('#pokemonName');
	await expect(nameLocator).toHaveText('Tinkatink');

	expect(
		page.locator('#evolutions div').filter({ hasText: 'Not in Black & White' }).nth(1)
	).toHaveText('Not in Black & White');
	expect(
		page.locator('#evolutions div').filter({ hasText: 'Not in Black & White' }).nth(2)
	).toHaveText('Not in Black & White');
});