import { expect } from '@playwright/test';
import { BaseHelper } from './base.helper';
import chalk from 'chalk';

export class FillEmployeeCreationForm extends BaseHelper {
    constructor(page) {
        super(page);
    }
    async verifyAddEmployeeForm(fieldName) {
        await this._page.waitForTimeout(2000);
        const parentLocator = this._page.locator(
            `//div[@role="dialog"]//span[text()="${fieldName}"]/parent::label/parent::div`
        );
        await expect(
            parentLocator,
            chalk.red(`Dialog ${fieldName} visibility`)
        ).toBeVisible();
    }

    async fillEmployeeForm(employeeInfo) {
        await this.fillText(employeeInfo.name, {
            name: 'name',
        });
        await this.fillText(employeeInfo.email, {
            name: 'email',
        });
        await this.fillText(employeeInfo.employee_code, {
            name: 'identifier',
        });
        await this.selectOption({
            input: employeeInfo.department,
            placeholder: 'Select  Department',
        });

        if (await employeeInfo.designation)
            await this.selectOption({
                input: employeeInfo.designation,
                placeholder: 'Select Designation',
            });

        await this.selectOption({
            input: employeeInfo.grade,
            placeholder: 'Select Grade',
        });
        await this.selectOption({
            input: employeeInfo.reporting_manager,
            placeholder: 'Select Manager',
            exact: true,
        });
        // await this.selectOption({
        //     input: this.employeeInfo.approval_mananger,
        //     placeholder: 'Select approval Manager',
        //     exact: true,
        // });
    }
}