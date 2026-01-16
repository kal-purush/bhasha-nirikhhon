import { ObjectDto } from '@/types/common.types';
import { expect } from '@playwright/test';
import chalk from 'chalk';
import { BreadCrumbHelper } from '../BaseHelper/breadCrumb.helper';
import { DialogHelper } from '../BaseHelper/dialog.helper';
import { FileHelper } from '../BaseHelper/file.helper';
import { NotesHelper } from '../BaseHelper/notes.helper';
import { TabHelper } from '../BaseHelper/tab.helper';
import CreateFinopsBusinessHelper from './createFinopsBusiness.helper';

export class BusinessDetailsPageHelper extends CreateFinopsBusinessHelper {
    public breadCrumb: BreadCrumbHelper;
    public businessInfo;
    public dialog: DialogHelper;
    public tab: TabHelper;
    public fileUpload: FileHelper;
    public notesHelper: NotesHelper;
    constructor(businessInfo, page: any) {
        super(page);
        this.breadCrumb = new BreadCrumbHelper(page);
        this.businessInfo = businessInfo;
        this.dialog = new DialogHelper(page);
        this.tab = new TabHelper(page);
        this.fileUpload = new FileHelper(page);
        this.notesHelper = new NotesHelper(page);
    }

    public async attachBankAccount() {
        await this.formHelper.submitButton();
        await this._page.waitForLoadState('networkidle');
    }

    public async parentRowLocator() {
        const row = await this.listHelper.findRowInTable(
            this.businessInfo.gstin,
            'GSTIN'
        );
        return row;
    }

    public async getBusiness(columnName: string) {
        const parentRow = await this.parentRowLocator();
        const business = await this.listHelper.getCell(parentRow, columnName);
        return business;
    }

    public async clickBusiness(columnName: string) {
        const parentRow = await this.getBusiness(columnName);
        let businessLink;
        switch (columnName) {
            case 'NAME':
                businessLink = parentRow.locator('//a');
                break;
            case 'GSTIN':
                businessLink = parentRow.locator('//div');
                break;
        }
        await businessLink.click();
    }

    public async getBusinessGstStatus(title: string) {
        const parentContainer = this.locate(
            '//div[contains(@class,"grid grid-cols-2")]'
        )._locator;

        const titleLocator = parentContainer.locator(
            `//p[text()="${title}"]/following-sibling::p`
        );
        return titleLocator.innerText();
    }

    public async informationDetailsLocator() {
        return this.locate("//div[@data-title='detail_information']")._locator;
    }

    public async getHeading() {
        const parentContainer = await this.informationDetailsLocator();
        return parentContainer.locator('h3');
    }

    public async verifyHeading(heading: string) {
        const heading_element = await this.getHeading();
        expect(heading_element, {
            message: 'Verifying Heading',
        }).toHaveText(heading);
    }

    public async checkInformation(title: string) {
        const parentContainer = await this.informationDetailsLocator();
        const titleLocator = parentContainer.locator(
            `//p[text()='${title}']/parent::div/parent::div/parent::div //span[contains(@id,"has-")]`
        );

        return titleLocator;
    }
    public async openGstinFiling(gstin: string) {
        console.log('gstin number', gstin);
        const parentContainer = await this.informationDetailsLocator();
        const locator = parentContainer.locator(
            `//span[text()='GSTIN']/parent::div/parent::p/parent::div/parent::div/parent::div //div[text()='${gstin}']`
        );

        await locator.click();
        await this._page.waitForLoadState('domcontentloaded');
        await this._page.waitForLoadState('networkidle');
    }

    public async verifyGstinFilingInformation(gstinInfo: ObjectDto) {
        await this.openGstinFiling(gstinInfo.gstin);
        await this.dialog.checkDialogTitle('GST Filing Status');
        const getStatus = await this.getBusinessGstStatus('Business Type');
        expect(getStatus).toBe(gstinInfo.business_type);
        await this.dialog.closeDialog();
    }

    public async verifyInformation(title: string, value: string) {
        const title_element = await this.checkInformation(title);
        expect(title_element, {
            message: 'Verify Information',
        }).toHaveText(value);
    }

    public async editInformation(title: string) {
        const parentContainer = await this.informationDetailsLocator();
        const titleLocator = parentContainer.locator(
            `//p[text()='${title}']/parent::div/parent::div/parent::div`
        );
        const editButton = titleLocator.locator('//a[text()="Edit"]');
        await editButton.click();
    }

    public async parentBusinessDetailsRow(name: string) {
        const row = await this.listHelper.findRowInTable(name, 'NAME');
        return row;
    }

    public async getBusinessDetails(name: string) {
        const parentRow = await this.parentBusinessDetailsRow(name);
        const business_gst = await this.listHelper.getCell(parentRow, 'NUMBER');
        return await business_gst.innerText();
    }

    public async getBusinessDocuments(name: string) {
        const parentRow = await this.parentBusinessDetailsRow(name);
        const business_gst = await this.listHelper.getCell(
            parentRow,
            'NO. OF DOCUMENTS'
        );
        return business_gst.locator('//span').isVisible();
    }

    public async clickBusinessAction(name: string) {
        const parentRow = await this.parentBusinessDetailsRow(name);
        const business_gst = await this.listHelper.getCellButton(
            parentRow,
            'ACTION'
        );
        await business_gst.click();
    }

    public async contactPersonParent(email: string) {
        const row = (
            await this.listHelper.findRowInTable(email, 'EMAIL')
        ).first();
        return row;
    }

    public async getContactPerson(email: string, columnName: string) {
        const parentRow = await this.contactPersonParent(email);
        const business_gst = (
            await this.listHelper.getCell(parentRow, columnName)
        ).first();
        return await business_gst.innerText();
    }

    public notesParent(notesTitle: string) {
        return this.locate(
            `//div[text()[normalize-space()='${notesTitle}']]/parent::div/parent::div`
        )._locator.first();
    }

    public async getNotesAuthor(notesTitle: string) {
        const parentLocator = this.notesParent(notesTitle);
        const author = await parentLocator.locator('//p[1]').innerText();
        return author;
    }

    public async checkNotesDate(notesTitle: string) {
        const parentLocator = this.notesParent(notesTitle);
        const date_time = parentLocator.locator('//p[2]');
        await expect(
            date_time,
            chalk.red('Date time visibility')
        ).toBeVisible();
    }

    async optionsParentLocator() {
        return this._page.locator(
            '//div[contains(@class,"breadcrumbs")]/parent::div'
        );
    }

    async clickEditIcon() {
        await this._page.waitForTimeout(2000);
        const parentLocator = await this.optionsParentLocator();
        const edit_button = parentLocator.locator(
            '//button[@data-title="Edit"]'
        );
        if (await edit_button.isVisible()) await edit_button.click();
    }

    // public async getBusinessName() {
    //     const parentRow = await this.parentRowLocator();
    //     const business_name = await this.listHelper.getCell(parentRow, 'NAME');
    //     return business_name;
    // }
    // public async getBusinessGST() {
    //     const parentRow = await this.parentRowLocator();
    //     const business_gst = await this.listHelper.getCell(parentRow, 'GSTIN');
    //     return business_gst;
    // }

    // public async getBusinessStatus() {
    //     const parentRow = await this.parentRowLocator();
    //     const business_status = await this.listHelper.getCell(
    //         parentRow,
    //         'STATUS'
    //     );
    //     return business_status;
    // }

    // public async getActiveVendor() {
    //     const parentRow = await this.parentRowLocator();
    //     const active_vendor = await this.listHelper.getCell(
    //         parentRow,
    //         'ACTIVE VENDORS'
    //     );
    //     return active_vendor;
    // }

    // public async getDateAdded() {
    //     const parentRow = await this.parentRowLocator();
    //     const date_added = await this.listHelper.getCell(parentRow, 'ADDED AT');
    //     return date_added;
    // }
}