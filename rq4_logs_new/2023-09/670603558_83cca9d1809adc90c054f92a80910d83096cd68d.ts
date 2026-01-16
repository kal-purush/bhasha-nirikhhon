import { BaseHelper } from '../BaseHelper/base.helper';
import { expect } from '@playwright/test';
import { VerifyEmailHelper } from '../SignupHelper/verifyEmail.helper';

export class VerifyPhone extends BaseHelper {
    private static VERIFY_EMAIL_DOM_SELECTOR =
        "(//div[contains(@class,'flex-1 h-full')])[1]";

    public async init(number: string) {
        await this.navigateTo(`VERIFYOTP`, number);
    }

    public async fillOtp(data: string) {
        for (let i = 0; i < 4; i++) {
            // Locate the OTP input fields and fill them with the corresponding digit
            await this._page.locator('.otpInput').nth(i).fill(data[i]);
        }
    }

    public async clickVerify() {
        await this.click({ role: 'button', name: 'Verify & Proceed' });
        await this._page.waitForTimeout(1000);
    }
    // public async withOutOtp() {
    //     const Btn_selector = `//button[@type='submit' and contains(@class, 'btn btn-primary')]`;
    //     const Btn = await this._page.locator(Btn_selector);
    //     const isDisabled = await Btn.getAttribute('disabled');
    //     expect(isDisabled).toBe('true');
    // }

    public async errorMessage() {
        return await this._page
            .locator('//div[contains(@class, "error-toast")]')
            .textContent();
    }
}