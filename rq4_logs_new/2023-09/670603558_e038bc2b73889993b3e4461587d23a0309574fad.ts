import { expect } from '@playwright/test';
import { BaseHelper } from '../../BaseHelper/base.helper';
import { LISTING_ROUTES } from '@/constants/api.constants';
import { vendorGstinInfo } from '@/utils/required_data';
import chalk from 'chalk';
import { DialogHelper } from '../../BaseHelper/dialog.helper';
import { FormHelper } from '../../BaseHelper/form.helper';

export class BusinessManagedOnboarding extends BaseHelper {
    public form: FormHelper;
    public dialog: DialogHelper;
    constructor(page: any) {
        super(page);
        this.dialog = new DialogHelper(page);
        this.form = new FormHelper(page);
    }
    public vendorBusiness;
    public ignore_next_page: string[] = [];

    async clickVendor(linkName: string) {
        const partyHover = this._page.getByText('Partiesarrow_drop_down');
        const partyClick = this._page
            .locator('a')
            .filter({ hasText: linkName })
            .nth(1);
        await partyHover.hover();
        await partyClick.click();
        await this._page.waitForTimeout(2000);
    }
    async verifyVendorPageURL() {
        await expect(this._page, chalk.red('Invite vendor URL')).toHaveURL(
            LISTING_ROUTES.VENDORS
        );
    }
    async verifyAddIcon() {
        const addIcon = this._page.locator(
            "//div[contains(@class,'flex-wrap justify-end')]//button[1]"
        );

        expect(
            await addIcon.isVisible(),
            chalk.red('Add Vendor Icon visibility')
        ).toBe(true);
    }
    async clickAddIcon() {
        await this.verifyAddIcon();
        await this._page
            .locator(
                "//div[contains(@class,'flex-wrap justify-end')]//button[1]"
            )
            .click();
    }

    // async verifyDialog() {
    //     expect(
    //         await this._page
    //             .locator("//div[text()='Add Vendor Account']")
    //             .isVisible(),
    //         chalk.red('Add vendor account visibility')
    //     ).toBe(true);
    // }

    async clickNavigationTab(nav: string) {
        await this._page.locator(`//span[text()='${nav}']`).click();
    }

    async validateCheckbox() {
        const checkbox = this.locate("//input[@type='checkbox']")._locator;
        expect(
            !(await checkbox.isChecked()),
            chalk.red('default checkbox state')
        ).toBe(true);
    }

    async afterSaveAndCreateValidation() {
        // if (!this.ignore_next_page.includes('move_to_next_page')) {
        const email_field = this.locate('input', {
            name: 'email',
        })._locator;
        expect(
            await email_field.inputValue(),
            chalk.red('Email field value')
        ).toBe('');
        const mobile_field = this.locate('input', {
            name: 'mobile',
        })._locator;
        expect(
            await mobile_field.inputValue(),
            chalk.red('Mobile Number field input value')
        ).toBe('');
        // }
    }

    async verifyBusinessManaged() {
        await this._page.waitForTimeout(1500);
        await this._page.waitForLoadState('networkidle');
        expect(
            await this.locate(
                '(//div[contains(@class,"text-center rounded")])[1]'
            )._locator.isVisible(),
            chalk.red('Business Managed visibility')
        ).toBe(true);

        expect(
            await this.locate(
                '(//div[contains(@class,"text-center rounded")])[1]'
            )._locator.innerText(),
            chalk.red('Business Managed match')
        ).toBe('Business Managed');
    }

    async verifyNonGstinStatus() {
        expect(
            await this.locate(
                '(//div[contains(@class,"text-center rounded")])[2]'
            )._locator.isVisible(),
            chalk.red('Non Gstin Status visibility')
        ).toBe(true);

        expect(
            await this.locate(
                '(//div[contains(@class,"text-center rounded")])[2]'
            )._locator.innerText(),
            chalk.red('Non Gstin Status match')
        ).toBe('Non GST Registered');
    }
}