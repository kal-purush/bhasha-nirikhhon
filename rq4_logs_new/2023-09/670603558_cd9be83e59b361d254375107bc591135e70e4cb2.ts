import { BaseHelper } from '@/baseHelper';
import { expect } from '@playwright/test';

export class NotesHelper extends BaseHelper {
    /**
     * @description Navigate to Notes page and Check Title of page
     *
     */

    public AddNotesConatiner() {
        return this.locate('//*[@id="radix-:r17:"]/div');
    }

    public async checkNotesPage() {
        const container = this.AddNotesConatiner().getLocator();
        const titleTexts = await container.locator('> h2').allInnerTexts();

        await expect(titleTexts[0]).toBe('Add Notes');
    }
}