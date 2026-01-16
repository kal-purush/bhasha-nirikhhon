import { Locator, expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';
import { formatDate } from '@/utils/common.utils';

export class UserCreation extends BaseHelper {
    public async init() {
        const isExpanded = await this._page
            .locator('.hamburger_button.hamburger_button--active')
            .isVisible();
        if (!isExpanded) {
            await this._page.locator('.hamburger_button').click();
        }
        await this._page
            .locator('a')
            .filter({ hasText: 'Configurations' })
            .click();
        await this._page.waitForTimeout(1000);
        await this._page
            .locator('.sidebar-item-title')
            .filter({ hasText: 'User Groups' })
            .click();
        await this._page.waitForTimeout(1000);
    }

    // open add user group form
    public async openUserGroupForm() {
        const container = this._page.locator(
            `//div[contains(@class,'breadcrumbs')]/ancestor::div[contains(@class,"justify-between")]`
        );
        await container
            .getByRole('button', { name: 'add_circle_outline Add User Group' })
            .click();
        console.log(chalk.green('Add User Group button clicked'));
        await expect(this._page.locator('//button[text()="Save"]'), {
            message: 'Checking save button visibility',
        }).toBeVisible();
        console.log(chalk.green('Add User Group form is visible'));
    }

    // verify popup on form
    public async verifyCancelPopup() {
        await this.fillInput('Test User Group', { name: 'name' });
        await this._page
            .getByRole('dialog')
            .locator('button')
            .filter({ hasText: 'Close' })
            .click();

        // check form popup options
        await this._page.waitForSelector("(//div[@role='dialog'])[2]");
        const dialog = this._page.locator("(//div[@role='dialog'])[2]");
        const yesButton = dialog.locator('button').getByText('Yes!');
        const noButton = dialog.locator('button').getByText('No');
        await expect(yesButton).toBeVisible();
        await expect(noButton).toBeVisible();

        // check if form is closed
        await yesButton.click();
        await expect(this._page.getByRole('dialog')).not.toBeVisible();

        // open form and check if data is cleared on cancel
        await this.openUserGroupForm();
        await this.fillInput('Test User Group', { name: 'name' });
        await this._page
            .getByRole('dialog')
            .locator('button')
            .filter({ hasText: 'Close' })
            .click();
        await noButton.click();
        await expect(this._page.getByRole('dialog').first()).toBeVisible();
        await expect(this._page.locator('input[name="name"]')).toHaveValue(
            'Test User Group'
        );
        await this.fillInput('', { name: 'name' });
    }

    // check error
    public async checkError(message: string, index: number) {
        console.log(chalk.blue('Checking error message and button disabled'));
        const error = this._page.locator('span.label.text-error').nth(index);
        expect(await error.textContent(), {
            message: `Checking: (${message}) error message`,
        }).toBe(message);
        console.log(chalk.green('Error message verified and button disabled'));
    }

    // get user group using a identifier from the table rows
    public async getRowFromTable(identifier: string) {
        const tableRow = this._page.locator('div.table-row.body-row');
        const count = await tableRow.count();
        let addedRow = null;
        for (let i = 0; i < count; i++) {
            const row = tableRow.nth(i);
            const rowText = await row.textContent();
            if (rowText.includes(identifier)) {
                addedRow = row;
                break;
            }
        }
        return addedRow;
    }

    // check user group addition in the row
    public async verifyUserGroupDetails(
        group: Locator,
        data: UserGroupData,
        status: string
    ) {
        expect(group).not.toBeNull();

        // verify name
        await expect(group).toContainText(data.name);

        // verify manager
        const manager = group.getByText(data.manager);
        expect(manager).not.toBeNull();

        // verify status
        const statusButton = group.locator('button').first();
        expect(await statusButton.textContent()).toBe(status);

        // verify description
        const description = group.getByText(data.description);
        expect(description).not.toBeNull();
    }

    // fill user group form
    public async fillUserGroupForm(data: UserGroupData) {
        console.log('data is here');
        console.log(data);

        await this._page.waitForSelector(
            '//div[@role="dialog"]/descendant::form'
        );
        if (data.name) {
            await this.fillInput(data.name, {
                name: 'name',
            });
        }
        if (data.manager) {
            await this.selectOption({
                input: data.manager,
                name: 'manager_id',
            });
        }
        if (data.description) {
            await this.fillText(data.description, {
                name: 'description',
            });
        }
        await this.click({ role: 'button', name: 'Save' });
        console.log(chalk.green('Save button clicked'));

        // check err message if name empty
        if (!data.name || !data.manager || !data.description) {
            console.log('check empty data');

            if (!data.name) {
                await this.checkError('Name is required', 0);
            }
            if (!data.manager) {
                await this.checkError('Manager is required', 1);
            }
            if (!data.description) {
                await this.checkError('Description is required', 2);
            }
            await expect(this._page.locator('//button[text()="Save"]'), {
                message: 'Checking save button visibility',
            }).toBeDisabled();
            return;
        }

        const toast = this._page.locator('div.ct-toast-success').first();
        expect(await toast.textContent(), {
            message: 'Checking toast message',
        }).toBe('Successfully saved');
        console.log(chalk.green('User Group toast message verified'));
    }

    // goto user group details page
    public async openUserGroupDetails(data: UserGroupData) {
        console.log(chalk.blue('Opening user group details'));
        await this._page.waitForSelector('div.table-row.body-row');
        await this._page
            .locator('div.table-row.body-row')
            .getByText(data.name, { exact: true })
            .click();
        await expect(
            this._page.getByText('User Group Detail').first()
        ).toBeVisible();
        console.log(chalk.green('User Group Detail page opened'));
        await expect(
            this._page.locator(`//div[text()="${data.name}"]`)
        ).toBeVisible();
        console.log(chalk.green('User Group name verified'));

        // verify description
        const desc = await this._page
            .locator('span#has-Description')
            .textContent();
        expect(desc).toBe(data.description);

        // verify manager
        const manager = await this._page
            .locator('span#has-Manager')
            .textContent();
        expect(manager).toBe(data.manager);
    }

    // navigate to tabs based on status
    public async navigateToTab(status: string) {
        console.log(chalk.blue('Navigating to tab:' + status));
        await this._page
            .locator('//button[@role="tab"]')
            .getByText(status, { exact: true })
            .click();
        await this._page.waitForTimeout(1000);
    }

    // get 100 rows with pagination
    public async toggleAll() {
        await this._page
            .locator('.selectbox-control')
            .filter({ hasText: '20 / Page' })
            .click();
        await this._page.getByText('100 / Page').click();
    }

    // toggle user group status from the row
    public async toggleStatus(name: string, status: string) {
        await this.navigateTo('USERGROUPS');
        await this.toggleAll();
        await this.navigateToTab('All');
        console.log(chalk.blue('Toggling group status'));
        const group = await this.getRowFromTable(name);
        const toggleButton = group.locator('button').first();
        let isClicked = false;
        if (
            (await toggleButton.textContent()) === 'Active' &&
            status === 'Inactive'
        ) {
            await toggleButton.click();
            isClicked = true;
        } else if (
            (await toggleButton.textContent()) === 'Inactive' &&
            status === 'Active'
        ) {
            await toggleButton.click();
            isClicked = true;
        } else {
            console.log(chalk.red('Group status is already ' + status));
        }
        if (isClicked) {
            const toast = this._page.locator('div.ct-toast-success').first();
            expect(await toast.textContent(), {
                message: 'Checking toast message',
            }).toBe('Status Changed');
            console.log(chalk.green('Toggle Status toast message verified'));
        }

        expect(toggleButton).toHaveText(status);
        await this._page.waitForTimeout(1000);
        console.log(chalk.green('Group status toggled'));
    }

    // verify if the user group is present in the table
    public async verifyIfPresent({
        data,
        present,
        status,
    }: {
        data: UserGroupData;
        present: boolean;
        status: string;
    }) {
        console.log(chalk.blue('user group listing page opened'));

        // check for no data case if present is false
        if (!present) {
            const noData = await this._page
                .locator("//div[@id='no-data-available']")
                .isVisible();
            if (noData) {
                return;
            }
        }

        await this._page.waitForSelector('div.table-row.body-row');
        const addedGroup = await this.getRowFromTable(data.name);

        // if added user group is not present
        if (!present) {
            expect(addedGroup).toBeNull();
            return;
        }
        await this.verifyUserGroupDetails(addedGroup, data, status);
    }
}