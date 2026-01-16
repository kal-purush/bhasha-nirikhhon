import { BaseHelper } from '../BaseHelper/base.helper';
import { expect } from '@playwright/test';

export class ExpenseHeadDetailsHelper extends BaseHelper {
    public async init() {
        await this.navigateTo('EXPENSE_HEADS');
    }

    public async clickOnExpenseHead(name: string) {
        this._page.getByRole('tab', { name: 'All', exact: true }).click();

        this._page.waitForTimeout(1000);

        await this._page.getByText(name).click();

        this._page.waitForTimeout(2000);
    }

    public async clickOnManagerName(name: string) {
        await this._page.getByText(name).click();

        this._page.waitForTimeout(2000);
    }

    public async clickOnEditIcon() {
        await this._page.locator('.items-center > button').first().click();

        this._page.waitForTimeout(2000);
    }
}