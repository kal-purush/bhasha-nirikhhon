import GenericGstinCardHelper from '@/helpers/CommonCardHelper/genericGstin.card.helper';
import { expect } from '@playwright/test';
import chalk from 'chalk';

export class VendorOnboardingWithGSTIN extends GenericGstinCardHelper {
    public static VENDORONBOARDINGWITHOUTGSTIN_DOM =
        '//div[text()="Documents for Approval"]';
    public async fillGstinInput(): Promise<void> {
        await expect(
            this._page.getByPlaceholder('ENTER GSTIN NUMBER'),
            chalk.red('Gstin input field visibility')
        ).toBeVisible();
        await this.fillText(this.gstin_data.gstin, {
            placeholder: 'ENTER GSTIN NUMBER',
        });
    }
    public async gstinDisplayName(displayName: string): Promise<void> {
        const display_name = await this._page
            .locator('#display_name')
            .inputValue();
        expect(display_name, chalk.red('Display name match with vendor')).toBe(
            displayName
        );
    }
}