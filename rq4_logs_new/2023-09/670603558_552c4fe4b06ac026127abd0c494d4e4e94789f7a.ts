import { BaseHelper } from './base.helper';

import { SignupHelper } from '../SignupHelper/signup.helper';
import { SignInHelper } from '../SigninHelper/signIn.helper';
import { Page } from '@playwright/test';
import { LISTING_ROUTES } from '@/constants/api.constants';

export class ProcessSignup extends BaseHelper {
    public userData = {
        username: '',
        password: '',
    };
    public signupHelper: SignupHelper;
    public signinHelper: SignInHelper;
    constructor(page: Page) {
        super(page);
        this.signupHelper = new SignupHelper(page);
        this.signinHelper = new SignInHelper(page);
    }

    public async newSignup() {
        await this.signupHelper.init();
        const username = SignupHelper.genRandomEmail();
        const name = 'test';
        const password = '123456';
        const confirm_password = '123456';

        await this.signupHelper.fillSignup({
            name: name,
            email: username,
            password: password,
            confirm_password: confirm_password,
        });

        await this.signupHelper.clickButton('Next →');

        await this.signupHelper.fillOtp('111111', 6);

        await this.signupHelper.clickButton('Verify →');
        await this.signupHelper.clickButton('Continue →');
        await this.signupHelper.fillInput(name, { name: 'organization_name' });

        await this.signupHelper.clickButton('Continue');

        this.userData.username = username;
        this.userData.password = password;

        return this.userData;
    }

    public async newLogin(url: keyof typeof LISTING_ROUTES) {
        await this.signinHelper.init();
        await this.signinHelper.checkDashboard({
            username: this.userData.username,
            password: this.userData.password,
        });

        await this.navigateTo(url);
    }
}