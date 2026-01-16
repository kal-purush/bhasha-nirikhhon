import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';

export type nonGstinDataType = {
    trade_name: string;
    display_name?: string;
    business_type: string;
    pin_code: string;
    address: string;
};
export default class GenericNonGstinCardHelper extends BaseHelper {
    public gstin_data: nonGstinDataType;
    public ignore_test_fields: string[] = [];

    constructor(gstin_data: nonGstinDataType, page: any) {
        super(page);
        this.gstin_data = gstin_data;
    }

    async checkBusinessName() {
        console.log('Checking Business Name', this.ignore_test_fields);
        const element = this.locate('div', {
            id: 'gstin_trade_name',
        })._locator;
        expect(await element.isVisible(), 'Gstin Trade Name not found').toBe(
            true
        );
        await expect(element, 'Gstin Trade Name not matched !!').toHaveText(
            this.gstin_data.trade_name
        );
    }
    // async checkGstinNumber() {
    //     const element = this.locate('div', {
    //         id: 'gstin_number',
    //     })._locator;

    //     expect(await element.isVisible(), 'Gstin Number not found').toBe(true);
    //     await expect(element, 'Gstin Number  not matched !!').toHaveText(
    //         this.gstin_data.value
    //     );
    // }
    async checkAddress() {
        const element = this.locate('span', {
            id: 'gstin_address',
        })._locator;

        expect
            .soft(await element.isVisible(), 'Gstin  Address not found')
            .toBe(true);

        await expect
            .soft(element, 'Gstin Address  not matched !!')
            .toHaveText(this.gstin_data.address);
    }
    async checkBusinessType() {
        const element = this.locate('span', {
            id: 'gstin_business_type',
        })._locator;
        expect(
            await element.isVisible(),
            'Gstin  business type not found'
        ).toBe(true);
        await expect(element, 'Gstin Business Type  not matched !!').toHaveText(
            this.gstin_data.business_type
        );
    }
    // async checkPAN() {
    //     const element = this.locate('span', {
    //         id: 'gstin_pan',
    //     })._locator;
    //     expect(await element.isVisible(), 'Pan number  not found').toBe(true);
    //     await expect(element, 'Gstin pan number  not matched !!').toHaveText(
    //         this.gstin_data.pan_number
    //     );
    // }

    // async checkGstinStatus() {
    //     const element = this.locate('div', {
    //         id: 'gstin_status',
    //     })._locator;

    //     expect(await element.isVisible(), 'Gstin Status  not found').toBe(true);
    //     await expect(element, 'Gstin status  not matched !!').toHaveText(
    //         this.gstin_data.status
    //     );
    // }

    async gstinInfoCheck() {
        await this._page.waitForTimeout(2000);
        if (!this.ignore_test_fields.includes('gstin_business_name'))
            await this.checkBusinessName();
        // if (!this.ignore_test_fields.includes('gstin_business_number'))
        //     await this.checkGstinNumber();
        if (!this.ignore_test_fields.includes('gstin_business_address'))
            await this.checkAddress();
        if (!this.ignore_test_fields.includes('gstin_business_business_type'))
            await this.checkBusinessType();
        // if (!this.ignore_test_fields.includes('gstin_business_pan'))
        //     await this.checkPAN();
        // if (!this.ignore_test_fields.includes('gstin_business_status'))
        //     await this.checkGstinStatus();
    }
}