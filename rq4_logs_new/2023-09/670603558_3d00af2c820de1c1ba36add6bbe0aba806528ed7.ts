import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';

export class PaymentModesHelper extends BaseHelper {
    public PaymentModeInfo;
    public PaymentMode_Update_Info;
    constructor(PaymentModeInfo, page) {
        super(page);
        this.PaymentModeInfo = PaymentModeInfo;
    }

    async init() {
        await this.navigateTo('PAYMENTMODES');
    }

    async verifyPaymentPage() {
        const getBreadcrumb = this.locate(
            '//div[contains(@class,"breadcrumbs")]'
        )._locator;
        expect(
            await getBreadcrumb.locator('//h1').textContent(),
            chalk.red('Payment Modes Page check')
        ).toContain('Payment Modes');
    }

    async addNewPaymentMode() {
        await this.click({ role: 'button', text: 'Add New' });
        expect(
            await this._page.getByRole('dialog').isVisible(),
            'dialog visibility'
        ).toBe(true);
    }
    async verifyEmptyField() {
        const name_field = await this.locate('input', {
            name: 'name',
        })._locator.inputValue();
        expect(name_field, chalk.red('Name field value')).toBe('');
    }

    async verifyAddNewPaymentMode() {
        const addPaymentHeading = await this.locate()
            ._locator.getByRole('heading')
            .textContent();
        expect(
            addPaymentHeading,
            chalk.red('Payment Mode Heading check')
        ).toContain('Add payment mode');
    }

    async fillPaymentModeForm() {
        await this.fillText(this.PaymentModeInfo.name, { name: 'name' });
        await this.selectOption({
            input: this.PaymentModeInfo.type ? this.PaymentModeInfo.type : '',
            placeholder: 'Select type',
        });

        const bankOption = await this.locate(
            '//div[@role="dialog"]//div[text()="Select Bank Account"]'
        )._locator.isVisible();

        if (bankOption == true) {
            await this.selectOption({
                input: this.PaymentModeInfo.bank,
                placeholder: 'Select Bank Account',
            });
        }
    }

    async searchPaymentMode() {
        await this.fillText(this.PaymentModeInfo.name, {
            placeholder: 'Search ( min: 3 characters )',
        });
        await this._page.waitForTimeout(2000);
    }

    async verifyPaymentDetails() {
        const parentHelper = this.locate(
            `//p[text()="${this.PaymentModeInfo.name}"]/parent::div/parent::div`
        )._locator.first();
        const bank_type = parentHelper.locator(
            `//p[text()="${this.PaymentModeInfo.type}"]`
        );

        if (
            ((await bank_type.innerText()) != 'Cash') == false ||
            ((await bank_type.innerText()) != 'Ledger') == false
        ) {
            const bank = parentHelper.locator(
                `//p[text()=${this.PaymentModeInfo.bank}]`
            );

            expect(
                await bank.isVisible(),
                chalk.red('Added Bank visibility')
            ).toBe(false);
        }

        const bank_status = parentHelper.locator('//button[text()="Active"]');
        const bank_added_date = parentHelper.locator('//div[@class="text-sm"]');
        const bank_action = parentHelper.locator('//button[@data-state]');

        await expect(bank_status, chalk.red('Status visibility')).toBeVisible();
        await expect(
            bank_added_date,
            chalk.red('Added Date visibility')
        ).toBeVisible();

        await expect(bank_action, chalk.red('Action visibility')).toBeVisible();
    }

    async changeBankStatus(status: string) {
        const parentHelper = this.locate(
            `//p[text()="${this.PaymentModeInfo.name}"]/parent::div/parent::div`
        )._locator.first();
        await parentHelper.locator(`//button[text()="${status}"]`).click();

        const dialogBox = this.locate('//div[@role="dialog"]');
        if (await dialogBox.isVisible()) {
            await this.click({ role: 'button', text: 'Yes!' });
        }
        await this._page.waitForTimeout(1000);
    }

    async verifyBankStatus(status: string) {
        const parentHelper = this.locate(
            `//p[text()="${this.PaymentModeInfo.name}"]/parent::div/parent::div`
        )._locator.first();
        const bank_status = await parentHelper
            .locator(`//button[text()="${status}"]`)
            .isVisible();
        expect(bank_status, chalk.red('Bank status visibility')).toBe(true);
    }

    async editPaymentModeName() {
        const parentHelper = this.locate(
            `//p[text()="${this.PaymentModeInfo.name}"]/parent::div/parent::div`
        )._locator.first();
        await parentHelper.locator('//button[@data-state]').click();

        const dialogBox = this.locate('//div[@role="dialog"]');
        if (await dialogBox.isVisible()) {
            await this.fillInput(this.PaymentModeInfo.updateName, {
                name: 'name',
            });
        }
        await this._page.waitForTimeout(1000);
    }
}