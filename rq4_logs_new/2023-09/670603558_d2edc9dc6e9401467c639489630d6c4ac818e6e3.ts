import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';

export class DesignationHelper extends BaseHelper {
    public designationInfo;
    constructor(designationInfo, page) {
        super(page);
        this.designationInfo = designationInfo;
    }
    async init() {
        await this.navigateTo('DESIGNATIONS');
    }
    async addDesignation() {
        await this._page
            .locator(
                "//div[contains(@class,'flex-wrap justify-end')]//button[1]"
            )
            .click();
    }

    async verifyDialog() {
        expect(
            this._page.getByRole('dialog'),
            'dialog is not visible'
        ).toBeVisible();
    }

    async verifyNameField() {
        await this._page.waitForTimeout(1000);
        const name_field = this.locate('input', { name: 'name' })._locator;
        expect(await name_field.isVisible(), 'Name field is not visible').toBe(
            true
        );
    }
    async fillNameField() {
        await this.verifyNameField();
        await this.fillText(this.designationInfo.name, { name: 'name' });
    }

    async verifyEmptyField() {
        const name_field = await this.locate('input', {
            name: 'name',
        })._locator.inputValue();
        expect(name_field, 'Name field is not empty').toBe('');
    }

    async searchDesignation() {
        await this.fillText(this.designationInfo.name, {
            placeholder: 'Search ( min: 3 characters )',
        });
        await this._page.waitForTimeout(2000);
    }

    async verifyItemInList() {
        const designationList = this._page.getByRole('link', {
            name: this.designationInfo.name,
            exact: true,
        });
        const itemStatus = this._page.getByRole('button', {
            name: 'Active',
        });
        // .filter({ hasText: this.designationInfo.name });
        const itemDate = this.locate(
            '(//div[@class="table-cell align-middle"]/following-sibling::div)[2]'
        )._locator;
        const itemAction = this.locate(
            '(//div[contains(@class,"icon-container transition-all")]//div)[1]'
        );
        expect(await designationList.isVisible(), 'Item not visible in').toBe(
            true
        );
        expect(await itemStatus.isVisible(), 'Item status is not visible').toBe(
            true
        );
        expect(await itemDate.isVisible(), 'Item date is not visible').toBe(
            true
        );
        expect(await itemAction.isVisible(), 'Item Action is not visible').toBe(
            true
        );
    }

    async verifyDesignationPage() {
        await expect(
            this._page.locator("(//p[text()='Designation Detail'])[1]"),
            'Designation page is not visible'
        ).toBeVisible();
        const designationListItem = this._page.locator(
            '//div[contains(@class,"text-base font-semibold")]'
        );
        expect(
            await designationListItem.textContent(),
            'Designation Name not matched'
        ).toBe(this.designationInfo.name);
    }

    async clickDesignationList() {
        const designationList = this._page.getByRole('link', {
            name: this.designationInfo.name,
            exact: true,
        });
        await designationList.click();
        await this._page.waitForTimeout(1000);
    }

    async changeStatus() {
        // const parentLocator = this._page.locator('div', {
        //     hasText: this.designationInfo.name,
        // });
        // console.log('parentLocator: ', parentLocator);
        const itemStatus = this._page
            .getByRole('button', {
                name: 'Active',
            })
            .first();
        // .filter({ hasText: this.designationInfo.name });

        expect(
            await itemStatus.isVisible(),
            'Item not found in Active list'
        ).toBe(true);
        if (await itemStatus.isVisible()) await itemStatus.click();
        await this.verifyChangeStatus();
    }
    async verifyChangeStatus() {
        const itemStatus = this._page
            .getByRole('button', {
                name: 'Inactive',
            })
            .first();
        expect(
            await itemStatus.isVisible(),
            'Item status does not changed to inactive'
        );
    }

    async changeTab(tabName: string) {
        await this._page
            .getByRole('tab', { name: tabName, exact: true })
            .click();
    }

    async clickAction() {
        const itemAction = this.locate(
            '(//div[contains(@class,"icon-container transition-all")]//div)[1]'
        );
        await itemAction.click();
    }
}