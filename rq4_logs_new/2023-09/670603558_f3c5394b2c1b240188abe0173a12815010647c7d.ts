import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';
import { formatDate } from '@/utils/common.utils';
import { DepartmentCreation } from './DepartmentCreation.helper';

export class DepartmentDetails extends DepartmentCreation {
    public async checkDepartmentDetailsDisplay({
        name,
        parent,
        manager,
        identifier,
    }: EmployeeCreationData) {
        console.log(chalk.green('Department details page is visible'));
        await expect(this._page.getByText(name, { exact: true })).toBeVisible();

        const identifierText = await this._page
            .locator('#has-Identifier')
            .textContent();
        expect(identifierText).toBe(identifier);

        const managerText = await this._page
            .locator('#has-Manager')
            .textContent();
        expect(managerText).toBe(manager);

        if (parent) {
            const parentText = await this._page
                .locator('#has-Parent')
                .textContent();
            expect(parentText).toBe(parent);
        }
    }

    public async openEditForm() {
        const container = this._page.locator(
            `//div[contains(@class,'breadcrumbs')]/ancestor::div[contains(@class,"justify-between")]`
        );
        await container.locator('button').first().click();
    }

    public async openAndVerifyActionButton() {
        await this._page.getByRole('button', { name: 'Actions' }).click();
        const dropdownItems = this._page.locator('//div[@role="menu"]');
        await expect(dropdownItems).toContainText('Employee Add');
        await expect(dropdownItems).toContainText('Add Notes');
        await expect(dropdownItems).toContainText('Add Documents');
    }

    public async openForm(option: string) {
        await this._page.getByRole('button', { name: 'Actions' }).click();
        await this._page
            .locator("//div[@role='menuitem']")
            .getByText(option)
            .click();
    }

    public async addEmployee(data: EmployeeCreationData) {
        await this.openForm('Employee Add');
        await this._page.waitForSelector(
            "//div[contains(text(), 'Select Designation')]"
        );

        await this.fillInput(data.name, { name: 'name' });
        await this.fillInput(data.email, { name: 'email' });
        await this.fillInput(data.identifier, { name: 'identifier' });
        await this.selectOption({
            option: data.manager,
            name: 'manager_id',
        });
        await this.clickButton('Save');
    }

    public async verifyEmployeeAddition(data: EmployeeCreationData) {
        const addedEmployeeRow = await this.getRowFromTable(data.identifier);
        expect(addedEmployeeRow).not.toBeNull();
        const name = await addedEmployeeRow.getByText(data.name);
        expect(name).not.toBeNull();
        const email = await addedEmployeeRow.getByText(data.email);
        expect(email).not.toBeNull();
        const identifier = await addedEmployeeRow.getByText(data.identifier);
        expect(identifier).not.toBeNull();
        const manager = await addedEmployeeRow.getByText(data.manager);
        expect(manager).not.toBeNull();
    }

    public async addDocument(document: { comment: string; imagePath: string }) {
        await this.openForm('Add Documents');
        await this._page
            .locator("//div[@role='presentation']")
            .locator("//input[@type='file']")
            .setInputFiles(`images/${document.imagePath}`);
        await this._page.waitForTimeout(1000);

        await this.fillText(document.comment, { name: 'comments' });
        await this.clickButton('Save');
    }

    public async verifyDocumentAddition(document: {
        comment: string;
        imagePath: string;
        date: Date;
    }) {
        await this._page.getByRole('tab').getByText('Documents').click();
        const documentsTab = this._page
            .locator("//div[@role='tabpanel']")
            .filter({ hasText: 'Documents' });
        await documentsTab.locator('button').nth(1).click();
        const addedRow = await this._page
            .locator("//div[contains(@class,'flex gap-4')]")
            .filter({
                hasText: document.comment,
            });
        expect(addedRow).not.toBeNull();
        await expect(addedRow).toContainText(document.comment);
        const formattedDate = formatDate(document.date);
        await expect(addedRow).toContainText(formattedDate);
    }

    public async addNotes(note: { title: string; date: Date }, open: boolean) {
        if (!open) await this.openForm('Add Notes');

        if (note.title) {
            await this.fillText(note.title, { name: 'comments' });
        }
        await this.clickButton('Save');
        if (!note.title) {
            console.log(
                chalk.blue('Checking error message and button disabled')
            );
            const error = this._page.locator('span.label.text-error').first();
            expect(await error.textContent(), {
                message: 'Checking error message',
            }).toBe('Notes is required');
            await expect(this._page.locator('//button[text()="Save"]'), {
                message: 'Checking save button visibility',
            }).toBeDisabled();
            console.log(
                chalk.green('Error message verified and button disabled')
            );
        }
    }

    public async verifyNoteAddition(note: { title: string; date: Date }) {
        await this._page.getByRole('tab').getByText('Notes').click();
        const notesTab = this._page.locator("//div[@role='tabpanel']").nth(2);
        const notesRow = notesTab.locator('div.flex.gap-4').filter({
            hasText: note.title,
        });
        expect(notesRow).not.toBeNull();
        const formattedDate = formatDate(note.date, true);
        const noteDate = notesRow.getByText(formattedDate);
        expect(noteDate).not.toBeNull();
    }
}