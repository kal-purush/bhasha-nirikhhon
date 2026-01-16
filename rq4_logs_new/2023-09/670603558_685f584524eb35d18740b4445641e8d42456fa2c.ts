import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import { LISTING_ROUTES } from '@/constants/api.constants';
interface gstinBusinessInformation {
    gstin: string;
    mobile: string;
    email: string;
}

export default class CreateFinopsBusinessHelper extends BaseHelper {
    async init() {
        const URL = LISTING_ROUTES['BUSINESSESS'];
        await this._page.goto(URL); // go to business page
        await this._page.waitForTimeout(1000);
    }

    async openBusinessForm() {
        const button = await this.locateByText('Add Business');

        expect(
            await button.isVisible(),
            'Add Business button not found !! '
        ).toBe(true);

        await button.click();
        await this._page.waitForTimeout(1000);
    }

    async checkModalHeaderTitle(element: any) {
        const header_element = await element.locateByText('Add Business', {
            selector: 'div',
        });

        expect(
            await header_element.isVisible(),
            'Add Business header not found in form  !!'
        ).toBe(true);
    }
    async checkFormIsOpen() {
        // const element = await this.locateByText('Add Business');
        const element = await this.locate('div', {
            role: 'dialog',
        });
        expect(await element.isVisible(), 'Form is not opened !!').toBe(true);

        // this.checkModalHeaderTitle(element);
    }

    async fillBusinessInputInformation(data: gstinBusinessInformation) {
        await this.fillInput(data?.gstin, { name: 'gstin' });
        await this.fillInput(data?.mobile, { name: 'mobile' });
        await this.fillInput(data?.email, { name: 'email' });
    }
}