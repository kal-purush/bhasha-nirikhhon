import { BaseHelper } from '../BaseHelper/base.helper';

import { expect } from '@playwright/test';

export class ForgotPasswordHelper extends BaseHelper {
    private FORGOTPASSWORD_DOM_SELECTOR = "//*[@id='__next']/div/div[1]/div";

    private Resend_btn_selector =
        "//*[@id='__next']/div/div[1]/div/div/div[3]/div/div[2]/span";

    public async init() {
        await this.navigateTo('FORGOTPASSWORD');
    }

    public async forgotPasswordPage(email: string) {
        await this._page.waitForSelector(this.FORGOTPASSWORD_DOM_SELECTOR);
        await this.fillText(email, { id: 'email' });
        await this.click({ role: 'button', name: 'Next →' });
        await this._page.waitForTimeout(1000);

        const resendButtonExists = await this._page
            .waitForSelector(this.Resend_btn_selector, { timeout: 5000 })
            .then(() => true)
            .catch(() => false);

        if (resendButtonExists) {
            await this.click({ role: 'button', name: 'Resend' });
            await this._page.waitForTimeout(1000);
        }

        await this.click({ role: 'button', name: 'Back' });
        await this._page.waitForTimeout(1000);
    }

    public async errorMessage() {
        return this._page
            .locator('//span[contains(@class, "label-text")]')
            .textContent();
    }
}