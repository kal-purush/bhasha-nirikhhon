import chalk from 'chalk';
import { BaseHelper } from '../BaseHelper/base.helper';
import { BreadCrumbHelper } from '../BaseHelper/breadCrumb.helper';
import { DialogHelper } from '../BaseHelper/dialog.helper';
import { FormHelper } from '../BaseHelper/form.helper';
import { ListingHelper } from '../BaseHelper/listing.helper';
import { NotificationHelper } from '../BaseHelper/notification.helper';
import { StatusHelper } from '../BaseHelper/status.helper';
import { TabHelper } from '../BaseHelper/tab.helper';
import { expect } from '@playwright/test';
import { DesignationHelper } from './DesignationCreation.helper';

export class DesignationDetailsPageHelper extends DesignationHelper {
    public employeeInfo;

    constructor(employeeInfo, page: any) {
        super(page);
        this.employeeInfo = employeeInfo;
    }

    public async optionsParentLocator() {
        return this._page.locator(
            '//div[contains(@class,"breadcrumbs")]/parent::div'
        );
    }
    public async verifyOptions() {
        const parentLocator = await this.optionsParentLocator();
        await expect(
            parentLocator.locator('.breadcrumbs'),
            chalk.red('breadcrumbs visibility')
        ).toBeVisible();

        const editIconButton = parentLocator.locator('//button[1]');
        await expect(
            editIconButton,
            chalk.red('Action button visibility')
        ).toBeVisible();

        const actionButton = parentLocator.locator(
            '//button[text()="Actions"]'
        );
        await expect(
            actionButton,
            chalk.red('Action Button Visibility')
        ).toBeVisible();
    }

    public async clickEditIcon() {
        const parentLocator = await this.optionsParentLocator();
        const edit_button = parentLocator.locator('//button[1]');
        if (await edit_button.isVisible()) await edit_button.click();
    }

    private async _employeeTabParent(name: string) {
        const row = await this.listing.findRowInTable(name, 'IDENTIFIER');
        return row;
    }
    public async verifyEmployeeTab(name: string, columnName: string) {
        const parentRow = await this._employeeTabParent(name);
        const cell = await this.listing.getCell(parentRow, columnName);
        await expect(cell).toBeVisible();
    }
    // public async verifyEmployeeTabDetails() {
    //     await this.locate('//button[text()="Employee"]')._locator.click();
    //     const parentHelper = this.locate(
    //         `//a[text()="${this.employeeInfo.identifier}"]/parent::div/parent::div`
    //     )._locator;

    //     const employee_name = parentHelper.locator(
    //         `//p[text()="${this.employeeInfo.name}"]`
    //     );
    //     const employee_email = parentHelper.locator(
    //         `//p[text()="${this.employeeInfo.email}"]`
    //     );
    //     const employee_department = parentHelper.locator(
    //         `//a[text()="${this.employeeInfo.department_id}"]`
    //     );

    //     await expect(
    //         employee_name,
    //         chalk.red('Employee Name match check')
    //     ).toBeVisible();
    //     await expect(
    //         employee_email,
    //         chalk.red('Employee Email match check')
    //     ).toBeVisible();
    //     await expect(
    //         employee_department,
    //         chalk.red('Employee Department match check')
    //     ).toBeVisible();
    // }

    public async addNotes() {
        await this.fillText(this.employeeInfo.notes, { id: 'comments' });
    }

    public async verifyNotesTabDetails(notes: string) {
        // await this.click({ role: 'button', text: 'Notes' });
        await this.locate('//button[text()="Notes"]')._locator.click();
        const parentHelper = this.locate(
            `//div[text()="${notes}"]/parent::div/parent::div`
        )._locator;
        const notes_authur = parentHelper.locator('//p[1]').first();
        const notes_added_date = parentHelper.locator('//p[2]').first();

        await expect(
            notes_authur,
            chalk.red('Notes Authur visibility')
        ).toBeVisible();

        await expect(
            notes_added_date,
            chalk.red('Notes Added Date visibility')
        ).toBeVisible();
    }

    public async uploadDocuments() {
        await this._page.setInputFiles(
            "//input[@type='file']",
            `./images/${this.employeeInfo.IMAGE_NAME}`
        );
        await this.fillText(this.employeeInfo.comments, { id: 'comments' });

        await this.click({ role: 'button', text: 'Save' });
        await this.click({ role: 'button', text: 'Save' });
    }

    public async verifyDocumentsTabDetails() {
        // await this.click({ role: 'button', text: 'Documents' });
        await this.locate('//button[text()="Documents"]')._locator.click();
        const parentHelper = this.locate(
            '//div[text()="Documents"]/parent::div/parent::div'
        )._locator;

        const imageName = parentHelper.locator(
            `//div[text()="${this.employeeInfo.IMAGE_NAME}"]`
        );

        await expect(
            imageName,
            chalk.red('Image Name check visibility')
        ).toBeVisible();
    }

    // async verifyEmployeeTab() {
    //     const parentIdentifierLocator = this._page.locator(
    //         `//a[text()="${this.employeeInfo.employee_code}"]/parent::div/parent::div`
    //     );

    //     const locateName = parentIdentifierLocator.locator('//p');
    //     const locateDepartment = parentIdentifierLocator.locator('');
    //     const locateEmail = parentIdentifierLocator.locator('');
    // }
}