import { ListingHelper } from '../BaseHelper/listing.helper';
import { expect } from '@playwright/test';

export class DocumentspreferenceHelper extends ListingHelper {
    public async init() {
        await this.navigateTo('DOCUMENT_PREFERENCES');
    }

    public async searchTextInList(name: string) {
        await this.searchInList(name);

        await this._page.waitForTimeout(1000);

        const row = await this.findRowInTable(name, 'NAME');

        const text = await this.getCellText(row, 'NAME');

        expect(text).toBe(name);
    }
}