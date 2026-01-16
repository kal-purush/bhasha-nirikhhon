import { Page } from "./page";

export class SignupPage extends Page {

    public get reviewerRegistrationModal() {
        return $('app-register-reviewer');
    }

    public get firstNameInput() {
        return $('[formcontrolname="firstName"]');
    }

    public get lastNameInput() {
        return $('[formcontrolname="lastName"]');
    }

    public get emailInput() {
        return $('[formcontrolname="email"]');
    }

    public get passwordInput() {
        return $('[formcontrolname="password"]');
    }

    public get confirmPasswordInput() {
        return $('[formcontrolname="confirmPassword"]');
    }

    public get agreeCheckbox() {
        return $('div.p-checkbox');
    }

    public get submitButton() {
        return $('button.primary-btn-full')
    }

    async shouldBeOpenedAsReviewer() {
        (await this.reviewerRegistrationModal)
            .waitForDisplayed({
                timeout: 6000
            });
    }
    
    async insertFirstName(options: {firstName: string}) {
        (await this.firstNameInput).setValue(options.firstName);
    }

    async insertLastName(options: {lastName: string}) {
        (await this.lastNameInput).setValue(options.lastName);
    }

    async insertEmail(options: {email: string}) {
        (await this.emailInput).setValue(options.email);
    }

    async insertPassword(options: {password: string}) {
        (await this.passwordInput).setValue(options.password);
    }

    async confirmPassword(options: {password: string}) {
        (await this.confirmPasswordInput).setValue(options.password);
    }

    async checkAgreeToS() {
        (await this.agreeCheckbox).click();
    }

    async submitRegistration() {
        (await this.submitButton).click();
    }

    async shouldErrorEmailMessageApear() {
        const errorMessage = $('//div[text() = " Valid email is required "]');
        (await errorMessage).waitForDisplayed({
            timeout: 6000,
            timeoutMsg: 'Email error does not appear'
        });
        await expect(errorMessage).toBeDisplayed();
    }

    async shouldErrorFirstNameAppear() {
        const errorMessage = $('//div[contains(text(), "First Name is required")]');
        (await errorMessage).waitForDisplayed({
            timeout: 6000,
            timeoutMsg: 'First Name error does not appear'
        });
        await expect(errorMessage).toBeDisplayed();
    }

    async shouldPasswordBeCorrect() {
        const errorMessage = $('//div[contains(text(), "Passwords must contain 8")]');
        (await errorMessage).waitForDisplayed({
            timeout: 6000,
            timeoutMsg: 'Password error does not appear'
        });
        await expect(errorMessage).toBeDisplayed();
    }

    async shouldPasswordsMatch() {
        const errorMessage = $('//div[contains(text(), "Passwords must match")]');
        (await errorMessage).waitForDisplayed({
            timeout: 6000,
            timeoutMsg: 'Password error does not appear'
        });
        await expect(errorMessage).toBeDisplayed();
    }

}