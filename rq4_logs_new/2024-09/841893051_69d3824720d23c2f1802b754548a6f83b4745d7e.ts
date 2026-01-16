import { expect, test } from '@playwright/test';

test.describe('Smoke test suite', () => {
	test.beforeEach(async ({ page }) => {
		console.log(`Running ${test.info().title}`);
		await page.goto('/');
	});

	test('should navigate to the about page', async ({ page }) => {
		await expect(page.locator('h2')).toContainText('Hi, ik ben Daniel Stals!');
	});

	test('dark or light mode should be toggled when theme toggle button is clicked', async ({ page }) => {
		const themeToggleButton = page.getByRole('button', { name: /toggle theme/i });
		const htmlElement = page.locator('html');
		await themeToggleButton.click();
		await expect(htmlElement).toHaveClass(/dark/);
		await themeToggleButton.click();
		await expect(htmlElement).toHaveClass(/light/);
	});

	test('chat window should open when input is focussed', async ({ page }) => {
		const formElement = page.getByRole('form', { name: /chat/i });
		await formElement.click();
		await expect(formElement).toHaveClass(/sm:h-\[400px\]/);
	});

	test('clicking a prompt suggestion should update chat input', async ({ page }) => {
		const promptSuggestionButtons = page.getByTestId('prompt-button');
		const firstPromptText = await promptSuggestionButtons.first().innerText();
		await promptSuggestionButtons.first().click();
		await expect(page.getByRole('textbox', { name: /chat-input/i })).toHaveValue(firstPromptText);
	});
});