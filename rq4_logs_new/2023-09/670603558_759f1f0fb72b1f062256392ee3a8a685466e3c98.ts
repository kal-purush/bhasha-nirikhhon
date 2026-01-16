import { Locator, Page, expect } from '@playwright/test';
import { BaseHelper } from '../BaseHelper/base.helper';
import chalk from 'chalk';
import {
    formatDate,
    formatDateProfile,
    generateRandomNumber,
} from '@/utils/common.utils';
import { ListingHelper } from '../BaseHelper/listing.helper';
import { DetailsPageHelper } from '../BaseHelper/details.helper';
import { TabHelper } from '../BaseHelper/tab.helper';
import { DocumentHelper } from '../BaseHelper/document.helper';
import { StatusHelper } from '../BaseHelper/status.helper';
import { LISTING_ROUTES, TEST_URL } from '@/constants/api.constants';
import { DialogHelper } from '../BaseHelper/dialog.helper';
import { NotificationHelper } from '../BaseHelper/notification.helper';
import { SetOrganization } from './SetOrg.helper';

export class ApprovalDelegation extends BaseHelper {
    private _dialogHelper: DialogHelper;
    private _notificationHelper: NotificationHelper;
    private _detailsPageHelper: DetailsPageHelper;
    private _listingHelper: ListingHelper;
    public tabHelper: TabHelper;
    private statusHelper: StatusHelper;

    constructor(page: Page) {
        super(page);
        this._dialogHelper = new DialogHelper(page);
        this._notificationHelper = new NotificationHelper(page);
        this._detailsPageHelper = new DetailsPageHelper(page);
        this._listingHelper = new ListingHelper(page);
        this.tabHelper = new TabHelper(page);
        this.statusHelper = new StatusHelper(page);
    }

    public async init() {
        await this.navigateTo('MYPROFILE');
    }

    public async openDelegationForm() {
        await this._detailsPageHelper.openActionButtonItem(
            'Add Approval Delegation'
        );
    }

    public async fillDelegationForm(data: ApprovalDelegationData) {
        await this.selectOption({
            option: data.DELEGATOR,
            name: 'delegated_id',
        });
        await this.fillText(data['START TIME'], { name: 'start_time' });
        await this.fillText(data['END TIME'], { name: 'end_time' });
        await this.fillText(data.COMMENTS, { name: 'comments' });
        // await this.clickButton('Save');
    }

    private async getRowWithDates(start_date: string, end_date: string) {
        const row = this._listingHelper
            .getTableContainer()
            .locator('div.table-row')
            .filter({
                hasText: start_date,
            })
            .filter({
                hasText: end_date,
            })
            .first();
        return row;
    }

    public async verifyDelegationAddition(data: ApprovalDelegationData) {
        const start_date = formatDateProfile(data['START TIME']);
        const end_date = formatDateProfile(data['END TIME']);
        const row = await this.getRowWithDates(start_date, end_date);

        const newData = {
            ...data,
            'START TIME': start_date,
            'END TIME': end_date,
        };
        await this._listingHelper.validateRow(row, newData);
    }

    public async verifyStatusChange(data: ApprovalDelegationData) {
        const start_date = formatDateProfile(data['START TIME']);
        const end_date = formatDateProfile(data['END TIME']);
        const row = await this.getRowWithDates(start_date, end_date);
        await this.statusHelper.setStatus('', 'Active', 'STATUS', row);
    }
}