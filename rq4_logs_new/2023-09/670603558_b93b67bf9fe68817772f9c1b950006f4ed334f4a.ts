import { BaseHelper } from '@/baseHelper';

export class FileHelper extends BaseHelper {
    /**
     * Retrieve the file container element.
     *
     * @return {HTMLElement} The file container element.
     */
    public getFileContainer() {
        return this.locate("//div[@role='presentation']");
    }

    /**
     * Sets the file input.
     *
     * @return {Promise<void>} Promise that resolves once the file is uploaded.
     */
    public async setFileInput() {
        const container = this.getFileContainer().getLocator();
        await container
            .locator("//input[@type='file']")
            .setInputFiles(`images/pan-card.jpg`);
        await this._page
            .locator('div.ct-toast-success')
            .waitFor({ state: 'visible' });
    }
}