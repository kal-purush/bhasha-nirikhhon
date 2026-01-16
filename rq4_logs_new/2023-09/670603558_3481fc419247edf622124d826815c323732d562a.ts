import { UserCreation } from './UserGroup.helper';
import { Locator, expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';
import { formatDate } from '@/utils/common.utils';

export class UserDetails extends UserCreation {
    public async checkGroupDetailsDisplay({
        name,
        manager,
        description,
    }: UserGroupData) {
        console.log(chalk.green('User Group details page is visible'));
        await expect(this._page.getByText(name, { exact: true })).toBeVisible();

        if (manager) {
            const managerText = await this._page
                .locator('#has-Manager')
                .textContent();
            expect(managerText).toBe(manager);
        }

        if (description) {
            const descriptionText = await this._page
                .locator('#has-Description')
                .textContent();
            expect(descriptionText).toBe(description);
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
        const dropdownItems = this._page
            .locator('//div[@role="menu"]')
            .locator('span');
        await expect(dropdownItems).toContainText([
            'Add Member',
            'Add Group Role',
            'Add Notes',
            'Add Documents',
        ]);
        // await expect(dropdownItems).toContainText('Add member');
        // await expect(dropdownItems).toContainText('Add Group Role');
        // await expect(dropdownItems).toContainText('Add Notes');
        // await expect(dropdownItems).toContainText('Add Documents');
    }

    public async openActionFormItem(option: string) {
        await this._page.getByRole('button', { name: 'Actions' }).click();
        await this._page
            .locator("//div[@role='menuitem']")
            .getByText(option)
            .click();
    }

    public async addMember(data: UserGroupData) {
        await this.openActionFormItem('Add Member');
        await this._page.waitForSelector("//span[contains(text(), 'Member')]");

        await this.selectOption({
            option: data.member,
            name: 'user_id',
        });
        await this.clickButton('Save');
    }

    public async verifyMemberAddition(data: UserGroupData) {
        const addedEmployeeRow = await this.getRowFromTable(data.member);
        expect(addedEmployeeRow).not.toBeNull();
    }

    public async addDocument(document: { comment: string; imagePath: string }) {
        await this.openActionFormItem('Add Documents');
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
        if (!open) await this.openActionFormItem('Add Notes');

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

    public async addRole(data: UserGroupData) {
        await this.openActionFormItem('Add Group Role');
        await this._page.waitForSelector("//span[contains(text(), 'Role')]");

        await this.selectOption({
            option: data.role,
            name: 'role_id',
        });
        await this.clickButton('Save');
    }

    public async deleteRole(data: UserGroupData) {
        await this._page.getByRole('tab').getByText('Roles').click();
        const addedRole = await this.getRowFromTable(data.role);
        await addedRole.locator('svg').first().click();
        await this._page
            .locator("//div[@role='dialog']")
            .locator('button')
            .getByText('Yes!')
            .click();
    }

    public async verifyRoleAddition(data: UserGroupData) {
        await this._page.getByRole('tab').getByText('Roles').click();
        const addedRole = await this.getRowFromTable(data.role);
        expect(addedRole).not.toBeNull();
    }
}