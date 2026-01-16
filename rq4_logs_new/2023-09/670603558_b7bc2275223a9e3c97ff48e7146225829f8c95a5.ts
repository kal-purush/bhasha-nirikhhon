import test from '@playwright/test';
import { PROCESS_TEST } from '@/fixtures';

import { DocumentspreferenceHelper } from '@/helpers/DocumentpreferenceHelper/documentspreference.helper';
test.describe('Configuration - Document Preference', () => {
    PROCESS_TEST('TDP003 - Document preference Details', async ({ page }) => {
        const documents_preference = new DocumentspreferenceHelper(page);
        const notification = await documents_preference.notificationHelper;

        await documents_preference.init();

        await PROCESS_TEST.step('Check page is opening', async () => {
            await documents_preference.checkPageTitle('Document Preferences');
        });

        await PROCESS_TEST.step('Check Data is present in list', async () => {
            await documents_preference.searchTextInList('COI');
        });

        await PROCESS_TEST.step('Check List Feild', async () => {
            await documents_preference.getTableHeader([
                'S.N',
                'NAME',
                'MANDATORY',
                'DOCUMENT MANDATORY',
            ]);
        });

        await PROCESS_TEST.step(
            'Change Document Mandatory Status',
            async () => {
                await documents_preference.changeSatus(
                    'COI',
                    'DOCUMENT MANDATORY'
                );

                await notification.checkToastSuccess('Status Changed');
            }
        );
        await PROCESS_TEST.step('Change Mandatory Status', async () => {
            await documents_preference.changeSatus('COI', 'MANDATORY');

            await notification.checkToastSuccess('Status Changed');
        });
    });
});