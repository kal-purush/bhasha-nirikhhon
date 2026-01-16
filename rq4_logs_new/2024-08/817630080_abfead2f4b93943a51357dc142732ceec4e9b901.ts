import { newE2EPage } from '@stencil/core/testing';

describe('ip-modal', () => {
  it('renders', async () => {
    const page = await newE2EPage();
    await page.setContent('<ip-modal></ip-modal>');

    const element = await page.find('ip-modal');
    expect(element).toHaveClass('hydrated');
  });

  it('should open modal when button is clicked', async () => {
    const page = await newE2EPage();
    await page.setContent(
      '<ip-modal button-text="open"><div slot="content">The content</div></ip-modal>',
    );

    const openButton = await page.find('ip-modal >>> .dialog-button');
    await openButton.click();
    await page.waitForChanges();

    const modal = await page.find('ip-modal >>> dialog');
    expect(modal).toHaveAttribute('open');

    const closeButton = await page.find('ip-modal >>> .close-dialog');
    await closeButton.click();
    await page.waitForChanges();
    expect(modal).not.toHaveAttribute('open');
  });
});