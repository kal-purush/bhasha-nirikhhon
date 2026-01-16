import { PROCESS_TEST } from '@/fixtures';
import { BusinessDetailsPageHelper } from '@/helpers/FinopsBusinessHelper/createFinopsBusiness.helper';
import { BANKDETAILS } from '@/utils/required_data';
import chalk from 'chalk';
import { tr } from 'date-fns/locale';

const { expect, describe } = PROCESS_TEST;

const businessInfo = {
    trade_name: 'Flipkart India Private Limited',
    value: '03AABCF8078M2ZA',
    business_type: 'Private Limited',
    email: 'user2@gmail.com',
    mobile: '9876543210',
    address: 'Village Katna, Teshil Payal Ludhiana Punjab',
    pan_number: 'AABCF8078M',
};

const updated_BusinessInfo = {
    email: 'updateduser@testing.com',
    mobile: '9876032123',
};
const AddNotes = {
    comments: 'Notes to be addded',
};
const notesSchema = {
    notes: {
        type: 'textarea',
        required: true,
    },
};

const BankInformation = {
    account_number: '123456789',
    re_account_number: '123456789',
    ifsc_code: 'ICIC0000004',
};

const BankInformationSchema = {
    account_number: {
        type: 'password',
        required: true,
    },
    re_account_number: {
        type: 'text',
        required: true,
    },
    ifsc_code: {
        type: 'text',
        required: true,
    },
};

const businessInfoSchema = {
    email: {
        type: 'email',
        required: true,
    },
    mobile: {
        type: 'number',
        required: true,
    },
};

describe('Business Detail', () => {
    const contactPersonInfo = {
        name: 'Ram Kumar Chhetri',
        mobile: '9876543321',
        email: 'testingperson2@test.com',
    };

    const contactPersonInfoSchema = {
        name: {
            type: 'text',
            required: true,
        },
        mobile: {
            type: 'number',
            required: true,
        },
        email: {
            type: 'email',
            required: true,
        },
    };

    PROCESS_TEST('TBD001', async ({ page }) => {
        const businessDetails = new BusinessDetailsPageHelper(
            businessInfo,
            page
        );
        await businessDetails.init();
        await businessDetails.breadCrumb.checkBreadCrumbTitle('My Businesses');

        await PROCESS_TEST.step('Check Business GST', async () => {
            await businessDetails.clickBusiness('GSTIN');
            await businessDetails.dialog.checkDialogTitle('GST Filing Status');
            const getStatus = await businessDetails.getBusinessGstStatus(
                'Business Type'
            );
            expect(getStatus).toBe(businessInfo.business_type);
            await businessDetails.dialog.closeDialog();
        });

        await PROCESS_TEST.step('Verify and Edit Email', async () => {
            await businessDetails.clickBusiness('NAME');
            await businessDetails.checkInformation('Email');
            await businessDetails.editInformation('Email');
            await businessDetails.dialog.checkDialogTitle('Edit Business');
            await businessDetails.formHelper.fillFormInputInformation(
                businessInfoSchema,
                businessInfo
            );
            await businessDetails.clickButton('Save');
            await businessDetails.checkToastSuccess('Successfully Saved');
        });

        await PROCESS_TEST.step('Verify and Edit Mobile Number', async () => {
            await businessDetails.checkInformation('Mobile Number');
            await businessDetails.editInformation('Mobile Number');
            await businessDetails.dialog.checkDialogTitle('Edit Business');
            await businessDetails.formHelper.fillFormInputInformation(
                businessInfoSchema,
                businessInfo
            );
            await businessDetails.clickButton('Save');
            await businessDetails.checkToastSuccess('Successfully Saved');
        });
        await PROCESS_TEST.step('Verify Address', async () => {
            const address = await businessDetails.checkInformation('Address');
            expect(address).toContain(businessInfo.address);
        });

        await PROCESS_TEST.step(
            'Verify and Edit Upload Documents',
            async () => {
                await businessDetails.tab.clickTab('Uploaded Documents');
                const gstinCert = await businessDetails.getBusinessDetails(
                    'GST Certificate'
                );
                const panCard = await businessDetails.getBusinessDetails(
                    'Pan Card'
                );

                expect(gstinCert, chalk.red('Gst Certificate check')).toContain(
                    businessInfo.value
                );
                expect(panCard, chalk.red('Pan Card check')).toContain(
                    businessInfo.pan_number
                );

                await PROCESS_TEST.step('Edit Documents', async () => {
                    await businessDetails.clickBusinessAction(
                        'GST Certificate'
                    );
                    await businessDetails.dialog.checkDialogTitle(
                        'Edit Document'
                    );
                    await businessDetails.fileUpload.setFileInput({
                        isDialog: true,
                    });
                    await businessDetails.clickButton('Save');
                    await businessDetails.checkToastSuccess(
                        'Successfully saved'
                    );
                    const documents =
                        await businessDetails.getBusinessDocuments('Pan Card');
                    expect(
                        documents,
                        chalk.red('Documents Visibility check')
                    ).toBe(true);
                });
            }
        );

        // PROCESS_TEST.skip('Connected Vendors - tab', async () => {
        //     await businessDetails.init();
        //     await businessDetails.clickBusiness('NAME');
        //     await businessDetails.tab.clickTab('Connected Vendors');
        //     const trade_name = await businessDetails.getBusiness('NAME');
        //     expect(trade_name, chalk.red('Connected Vendor check')).toBe(
        //         businessInfo.trade_name
        //     );
        // });

        await PROCESS_TEST.step('Bank Account - tab', async () => {
            await businessDetails.tab.clickTab('Bank Accounts');
            // await businessDetails.clickButton('Add Bank Account');
            await businessDetails.clickActionButton();
            await businessDetails.clickActionOption('Add Bank Account');
            await businessDetails.dialog.checkDialogTitle('Add Bank Account');
            await businessDetails.formHelper.fillFormInputInformation(
                BankInformationSchema,
                BankInformation
            );
            await businessDetails.fileUpload.setFileInput({ isDialog: true });
            await businessDetails.clickButton('Save');
            await businessDetails.clickButton('Save');
            await businessDetails.checkToastSuccess('Successfully saved');
        });

        await PROCESS_TEST.step('Contact Person - tab', async () => {
            await businessDetails.tab.clickTab('Contact Persons');
            // await businessDetails.clickButton('Add Contact Person');
            await businessDetails.clickActionButton();
            await businessDetails.clickActionOption('Add Contact Person');
            await businessDetails.dialog.checkDialogTitle('Add Contact Person');
            await businessDetails.formHelper.fillFormInputInformation(
                contactPersonInfoSchema,
                contactPersonInfo
            );
            await businessDetails.clickButton('Save');
            await businessDetails.checkToastSuccess('Successfully saved');

            await PROCESS_TEST.step('Verify Added Person Contact', async () => {
                const getName = await businessDetails.getContactPerson(
                    contactPersonInfo.email,
                    'NAME'
                );
                const getMobile = await businessDetails.getContactPerson(
                    contactPersonInfo.email,
                    'MOBILE'
                );
                expect(getName).toContain(contactPersonInfo.name);
                expect(getMobile).toContain(contactPersonInfo.mobile);
            });
        });

        await PROCESS_TEST.step('Add Notes - tab', async () => {
            await businessDetails.tab.clickTab('Notes');
            // await businessDetails.clickButton('Add Notes');
            await businessDetails.clickActionButton();
            await businessDetails.clickActionOption('Add Notes');
            await businessDetails.dialog.checkDialogTitle('Add Notes');
            await businessDetails.formHelper.fillTextAreaForm(AddNotes);
            await businessDetails.clickButton('Save');
            // await businessDetails.checkToastSuccess('Successfully saved');

            await PROCESS_TEST.step('Verify Added Notes', async () => {
                await businessDetails.getNotesAuthor(AddNotes.comments);
                await businessDetails.checkNotesDate(AddNotes.comments);
            });
        });

        await PROCESS_TEST.step('Edit Business Details', async () => {
            await businessDetails.clickEditIcon();
            await businessDetails.dialog.checkDialogTitle('Edit Business');

            await businessDetails.formHelper.fillFormInputInformation(
                businessInfoSchema,
                updated_BusinessInfo
            );
            businessDetails.formHelper.checkIsInputEditable({
                name: 'gstin',
            });
            await businessDetails.clickButton('Save');
            await businessDetails.checkToastSuccess('Successfully Saved');

            await PROCESS_TEST.step('Verify Updated Info', async () => {
                await businessDetails.checkInformation('Email');
                await businessDetails.checkInformation('Mobile Number');
            });
        });
    });
});