import { BaseHelper } from '@/baseHelper';
import { Locator, Page, expect } from '@playwright/test';
import { FileHelper } from './file.helper';
import { DocumentHelper } from './document.helper';

type DetailInfo = {
    selector: string;
    text: string;
};

export class DetailsPageHelper extends BaseHelper {
    public _documentHelper: DocumentHelper;

    constructor(page: Page) {
        super(page);
        this._documentHelper = new DocumentHelper(page);
    }

    /**
     * Retrieve the action button element.
     *
     * @return {HTMLElement} The document container element.
     */
    public getActionButton() {
        return this.locateByText('Actions', { role: 'button' });
    }

    /**
     * Retrieve the dropdown items in action button.
     *
     * @return {Locator} The document container element.
     */
    public getDropdownItems() {
        return this.locateByRole('menuitem').locator('span');
    }

    /**
     * Retrieve the document details container element.
     *
     * @return {Locator} The document details container element.
     */
    public getDetailInfoContainer() {
        return this._page.locator('.h-full.overflow-hidden.rounded.shadow-lg');
    }

    /**
     * Retrieve details item
     *@param {string} identifier - The identifier of the item.
     *@param {string} text - The text of the item.
     * @return {Locator} The document details container element.
     */
    public async validateDetailInfoItem(identifier: string, text: string) {
        const container = this.getDetailInfoContainer();
        const infoText = await container.locator(`${identifier}`).textContent();
        expect(infoText).toBe(text);
    }

    /**
     * Checks if the options are present in action button.
     *
     * @param {string[]} options - Options to be checked inside action button.
     * @return {Promise<void>} Promise that resolves once the options are checked.
     */
    public async checkActionButtonOptions(options: string[]) {
        await this.getActionButton().click();
        const dropdownItems = this.getDropdownItems();
        await expect(dropdownItems).toContainText(options);
        // unblur the action button
        await this.click({
            selector: 'html',
        });
    }

    /**
     * Opens the form using
     *
     * @param {string} options - Form to be opened inside action button.
     * @return {Promise<void>} Promise that resolves once the options are checked.
     */
    public async openActionButtonItem(option: string) {
        const actionButton = this.getDropdownItems().getByText(option);
        await actionButton.click();
    }

    /**
     * Checks if the options are present in action button.
     *
     * @param {string[]} options - Options to be checked inside action button.
     * @return {Promise<void>} Promise that resolves once the options are checked.
     */
    public async validateDetailsPageInfo(details: DetailInfo[]) {
        for (const detail of details) {
            await this.validateDetailInfoItem(detail.selector, detail.text);
        }
    }
}