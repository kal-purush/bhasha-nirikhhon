import { newE2EPage } from '@stencil/core/testing';

describe('ip-search-bar', () => {
  it('renders and responds to user input', async () => {
    const page = await newE2EPage();
    await page.setContent('<ip-search-bar></ip-search-bar>');

    const element = await page.find('ip-search-bar');
    expect(element).toHaveClass('hydrated');
  });

  it('handles keyboard navigation', async () => {
    const page = await newE2EPage();
    await page.setContent(
      '<ip-search-bar suggestions-data=\'["Apple", "Banana", "Cherry"]\' placeholder="Search..." label-button="Search"></ip-search-bar>',
    );

    const input = await page.find('ip-search-bar >>> input');
    await input.type('a');
    await page.waitForChanges();

    await input.press('ArrowDown');
    await page.waitForChanges();

    const highlightedItem = await page.find('ip-search-bar >>> .highlighted');
    expect(highlightedItem.textContent).toBe('Apple');

    await input.press('Enter');
    await page.waitForChanges();

    expect(await input.getProperty('value')).toBe('Apple');

    const suggestionList = await page.find(
      'ip-search-bar >>> #suggestion-list',
    );
    expect(suggestionList).toBeNull();
  });
});