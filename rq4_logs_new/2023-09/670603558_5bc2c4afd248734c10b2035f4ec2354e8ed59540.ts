import { expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';
import { VendorOnboardingWithGSTIN } from './VendorOnboarding.helper';
import { MSME_NUMBER, vendorGstinInfo } from '@/utils/required_data';

export class VendorInvitationDetails extends BaseHelper {
    public bankDetails;
    public lowerTDS;
    constructor(bankDetails, lowerTDS, page) {
        super(page);
        this.bankDetails = bankDetails;
        this.lowerTDS = lowerTDS;
    }
    private INVITATION_DETAILS_DOM =
        "(//div[contains(@class,'flex-1 gap-4')])[2]";

    public async checkFrom() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("(//div[contains(@class,'flex-1 gap-1')]//div)[1]")
            .textContent();
    }
    public async checkFromGSTIN() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        const businessGSTIN = helper._page.locator(
            "//span[contains(@class,'text-xs text-base-tertiary')]"
        );
        await expect
            .soft(businessGSTIN, chalk.red('GSTIN of Business visibility'))
            .toBeVisible();
        if (await businessGSTIN.isVisible())
            return await businessGSTIN.textContent();
    }
    public async checkClient() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("//span[contains(@class,'inline-block w-3/4')]")
            .textContent();
    }

    public async checkClientGSTIN() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("//span[contains(@class,'text-xs cursor-pointer')]")
            .textContent();
    }

    //For non GSTIN Information
    public async checkNonGstinFrom() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("(//div[@class='w-full']//span)[3]")
            .textContent();
    }
    public async checkGstinFromNonGstin() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        const businessGSTIN = helper._page.locator(
            "(//div[contains(@class,'text-center rounded')])[1]"
        );
        await expect
            .soft(businessGSTIN, chalk.red('GSTIN of Business visibility'))
            .toBeVisible();
        if (await businessGSTIN.isVisible())
            return await businessGSTIN.textContent();
    }

    public async checkNonGstinClient() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("//span[contains(@class,'inline-block w-3/4')]")
            .textContent();
    }

    public async checkNonGstinClientGSTIN() {
        const helper = this.locate(this.INVITATION_DETAILS_DOM);
        return await helper._page
            .locator("//span[contains(@class,'text-xs cursor-pointer')]")
            .textContent();
    }

    public async checkDocument(title: string) {
        const helper = this.locate(
            VendorOnboardingWithGSTIN.VENDORONBOARDINGWITHOUTGSTIN_DOM
        );
        const container = helper._page
            .locator(`(//div[contains(@class,'border-b cursor-pointer')])`)
            .filter({ hasText: title });
        const imageName = helper._page.locator(
            '//div[contains(@class,"overflow-hidden font-medium")]'
        );
        if (await container.isVisible()) {
            await container.click();
            if (title === 'GSTIN Certificate') {
                const gstinStatus = helper._page.locator(
                    "//p[text()='GST Status']/following-sibling::p"
                );

                expect(
                    await gstinStatus.isVisible(),
                    chalk.red('GSTIN Status visibility')
                ).toBe(true);
                expect(
                    await gstinStatus.textContent(),
                    chalk.red('GSTIN Status match')
                ).toBe(vendorGstinInfo.status);

                if (await imageName.isVisible()) {
                    expect(
                        imageName,
                        chalk.red('Image Name match')
                    ).toBeVisible();
                }
            }
            if (title === 'Pan Card') {
                if (await imageName.isVisible()) {
                    expect(
                        imageName,
                        chalk.red('Image Name match')
                    ).toBeVisible();
                }
            }

            if (title === 'MSME') {
                const msmeLocator = helper._page.locator(
                    "//div[text()='MSME number']/following-sibling::div"
                );
                expect(
                    await msmeLocator.isVisible(),
                    chalk.red('MSME visibility')
                ).toBe(true);

                expect(
                    await msmeLocator.textContent(),
                    chalk.red('MSME match')
                ).toBe(MSME_NUMBER);

                if (await imageName.isVisible()) {
                    expect(
                        imageName,
                        chalk.red('Image Name match')
                    ).toBeVisible();
                }
            }

            if (title === 'Lower TDS') {
                const tdsCertNumber = helper._page.locator(
                    "//div[text()='TDS Certificate Number']/following-sibling::div"
                );
                const tdsPercentage = helper._page.locator(
                    '//div[text()="Lower TDS Percentage"]/following-sibling::div'
                );
                console.log(
                    'Lower TDS Percentage: ' + (await tdsPercentage.innerText())
                );
                const expireDate = helper._page.locator(
                    '//div[text()="Expiry Date"]/following-sibling::div'
                );

                expect(
                    await tdsCertNumber.textContent(),
                    chalk.red('TDS Certificate Number match')
                ).toBe(this.lowerTDS.identifier);

                expect(
                    await tdsPercentage.textContent(),
                    chalk.red('TDS Percentage match')
                ).toBe(this.lowerTDS.custom_field_data.percentage + '%');

                expect(
                    await expireDate.textContent(),
                    chalk.red('Expiry Date match')
                ).toBe('22 Feb, 2023');

                if (await imageName.isVisible()) {
                    expect(
                        imageName,
                        chalk.red('Image Name match')
                    ).toBeVisible();
                }
            }

            if (title === 'Bank') {
                const bankName = helper._page.locator(
                    "//div[text()='Name']/following-sibling::div"
                );
                const accountNumber = helper._page.locator(
                    '//div[text()="A/C Number"]/following-sibling::div'
                );
                const ifscCode = helper._page.locator(
                    "//div[text()='IfSC Code']/following-sibling::div"
                );

                expect(
                    await bankName.textContent(),
                    chalk.red('Bank Name match')
                ).toBe(this.bankDetails.bankName);

                expect(
                    await accountNumber.textContent(),
                    chalk.red('Account Number match')
                ).toBe(this.bankDetails.account_number);

                expect(
                    await ifscCode.textContent(),
                    chalk.red('IFSC Code match')
                ).toBe(this.bankDetails.ifsc_code);
            }
        }
    }
}
/**
     * @todo -Need to add Expiry Date converter

     */