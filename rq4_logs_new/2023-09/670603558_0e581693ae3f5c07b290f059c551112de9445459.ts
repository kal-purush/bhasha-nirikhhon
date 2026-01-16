import { PROCESS_TEST } from '@/fixtures';
import { DialogHelper } from '@/helpers/BaseHelper/dialog.helper';
import { FormHelper } from '@/helpers/BaseHelper/form.helper';
import {
    CreateDesignationHelper,
    DesignationDetailsPageHelper,
    DesignationHelper,
} from '@/helpers/DesignationHelper/designation.helper';

import chalk from 'chalk';

const { expect, describe } = PROCESS_TEST;

describe('HR-Designations ', () => {
    const designationInfo_Save_And_Create = {
        name: 'save and create',
    };

    //Create Designation Info
    const Create_Designation_Info = {
        name: 'Testing Design022dd',
    };

    const designationUpdateInfo = {
        name: 'Testing Save_Creat039',
    };

    //Update Designation Info from Details Page
    const designationInfo = {
        name: designationUpdateInfo.name,
    };

    //designationInfo must be valid to change in details page
    const designation_details_page_Info = {
        name: Create_Designation_Info.name,
    };

    //Add Employee in Designation field
    const employeeInfo = {
        name: 'CTO',
        email: 'testemploye163344@email.com',
        identifier: 'EMPP163344',
        department_id: 'Test857730457030',
        grade_id: 'E2',
        manager_id: 'Amit Raj',
        approval_manager_id: 'Ravi',
        notes: 'Adding Notes for testing',
        IMAGE_NAME: 'pan-card.jpg',
        comments: 'Testing upload document comments',
    };
    const employeeInfoSchema = {
        name: {
            type: 'text',
            required: true,
        },
        email: {
            type: 'text',
            required: true,
        },
        identifier: {
            type: 'text',
            required: true,
        },
        department_id: {
            type: 'select',
        },
        grade_id: {
            type: 'select',
        },
        manager_id: {
            type: 'select',
        },
        approval_manager_id: {
            type: 'select',
        },
    };

    PROCESS_TEST('TDE001', async ({ page }) => {
        const designation = new DesignationHelper(
            designationInfo_Save_And_Create,
            page
        );
        const designationCreate = new CreateDesignationHelper(
            designationInfo,
            page
        );
        const formHelper = new FormHelper(page);
        await designation.init();

        await PROCESS_TEST.step('Save and Create Designation', async () => {
            await designationCreate.addDesignation();
            await designation.verifyDialog();
            await designation.fillNameField();
            await designationCreate.saveAndCreateCheckbox();
            await designation.clickButton('Save');
            await designation.notification.getErrorMessage();
            // expect(
            await designation.notification.checkToastSuccess(
                'Successfully saved'
            );
            //     chalk.red('Toast Message match')
            // ).toBe('Successfully saved ');
            await formHelper.isInputFieldEmpty('name');
        });

        await PROCESS_TEST.step('Create Designation', async () => {
            await designation.verifyDialog();
            await designation.fillNameField();
            await designation.clickButton('Save');
            await designation.notification.checkToastSuccess(
                'Successfully saved'
            );
            // expect(
            //     await designation.toastMessage(),
            //     chalk.red('Toast Message match')
            // ).toBe('Successfully saved ');
            await designation.searchDesignation();
            await designation.verifyItemInList();
            await designation.clickDesignationName();
            await designation.breadCrumb.checkBreadCrumbTitle(
                'Designation Detail'
            );
        });

        await PROCESS_TEST.step('Change Designation Status', async () => {
            await designation.searchDesignation();
            //Change from Active to Inactive
            await designationCreate.changeStatus('Active');

            await PROCESS_TEST.step('Verify Inactive Tab', async () => {
                //Change from Inactive to Active
                await designation.changeTab('Inactive');
                await designation.searchDesignation();
                await designationCreate.verifyChangeStatus('Inactive');

                // await designationCreate.changeStatus('Inactive');
                // await designation.changeTab('Active');
                // await designationCreate.verifyChangeStatus('Active');
            });

            await PROCESS_TEST.step('Verify Active Tab', async () => {
                await designationCreate.changeStatus('Inactive');
                await designation.changeTab('Active');
                await designationCreate.verifyChangeStatus('Active');
            });

            await PROCESS_TEST.step('Verifying All Tab', async () => {
                await designation.changeTab('All');
                await designation.searchDesignation();

                await expect(
                    page.getByRole('link', {
                        name: designationInfo.name,
                        exact: true,
                    }),
                    chalk.red('Designation Name visibility')
                ).toBeVisible();
            });
        });

        await PROCESS_TEST.step('Test Action Designation', async () => {
            const updateDesignation = new DesignationHelper(
                designationUpdateInfo,
                page
            );
            await designation.searchDesignation();
            await designation.changeTab('All');
            await designation.clickAction();

            await updateDesignation.fillNameField();
            await designation.clickButton('Save');
            await designation.notification.checkToastSuccess(
                'Successfully saved'
            );
            // expect(
            //     await designation.toastMessage(),
            //     chalk.red('Toast Message match')
            // ).toBe('Successfully saved ');
            await designation.changeTab('All');
            await updateDesignation.searchDesignation();
            await updateDesignation.verifyItemInList();
        });
    });

    PROCESS_TEST('TDED001', async ({ page }) => {
        const designation = new DesignationHelper(designationInfo, page);
        const detailsPage = new DesignationDetailsPageHelper(
            employeeInfo,
            page
        );
        const detailsPageEdit = new DesignationHelper(
            designation_details_page_Info,
            page
        );
        const dialog = new DialogHelper(page);

        await PROCESS_TEST.step('Verify Details Page', async () => {
            await designation.init();
            await designation.searchDesignation();
            await designation.changeTab('All');
            await designation.clickDesignationName();
            await designation.breadCrumb.checkBreadCrumbTitle(
                'Designation Detail'
            );
            await PROCESS_TEST.step('Verifying Options', async () => {
                await detailsPage.verifyOptions();
            });
        });

        await PROCESS_TEST.step('Edit name from details page', async () => {
            await detailsPage.clickEditIcon();

            await detailsPageEdit.fillNameField();
            await designation.clickButton('Save');
            await designation.notification.checkToastSuccess(
                'Successfully saved'
            );
            // expect(
            //     await designation.toastMessage(),
            //     chalk.red('ToastMessage match')
            // ).toBe('Successfully saved ');
        });

        await PROCESS_TEST.step('Verifying Action Options', async () => {
            await detailsPage.clickActionButton();
            await detailsPage.verifyActionOptions('Add Employee');
            await detailsPage.verifyActionOptions('Add Notes');
            await detailsPage.verifyActionOptions('Add Documents');
        });

        await PROCESS_TEST.step('Verify Employee Form Field', async () => {
            await detailsPage.clickActionOption('Add Employee');
            await dialog.verifyInputField('Name');
            await dialog.verifyInputField('Email');
            await dialog.verifyInputField('Employee Code');
            await dialog.verifyInputField('Department');
            await dialog.verifyInputField('Designation');
            await dialog.verifyInputField('Grade');
            await dialog.verifyInputField('Reporting Manager');
            await dialog.verifyInputField('Approval Manager');
        });

        await PROCESS_TEST.step('Fill Employee Form Field', async () => {
            const formHelper = new FormHelper(page);
            await formHelper.fillFormInputInformation(
                employeeInfoSchema,
                employeeInfo
            );
            // await detailsPage.fillEmployeeForm();
            await detailsPage.clickButton('Save');
            await designation.notification.checkToastSuccess(
                'Successfully created'
            );

            await PROCESS_TEST.step('Verify Employee Tab Details', async () => {
                await detailsPage.verifyEmployeeTabDetails();
            });
        });

        await PROCESS_TEST.step('Fill Notes', async () => {
            await detailsPage.clickActionButton();
            await detailsPage.clickActionOption('Add Notes');
            await detailsPage.addNotes();
            await detailsPage.clickButton('Save');
        });

        await PROCESS_TEST.step('Verify Notes Tab Details', async () => {
            await detailsPage.verifyNotesTabDetails();
        });

        await PROCESS_TEST.step('Add Documents', async () => {
            const detailsPage = new DesignationDetailsPageHelper(
                designation_details_page_Info,
                page
            );

            const detailsPageUploadDocuments = new DesignationDetailsPageHelper(
                employeeInfo,
                page
            );

            await PROCESS_TEST.step('Upload Documents', async () => {
                await detailsPage.clickActionButton();
                await detailsPage.clickActionOption('Add Documents');
                await detailsPageUploadDocuments.uploadDocuments();
                // await detailsPage.clickButton('Save');
            });
            await PROCESS_TEST.step(
                'Verify Documents Tab Details',
                async () => {
                    await detailsPageUploadDocuments.verifyDocumentsTabDetails();
                }
            );
        });
    });
});