import { ObjectDto } from '@/types/common.types';
import { Page, expect } from '@playwright/test';
import { FormHelper } from '../BaseHelper/form.helper';
import { ListingHelper } from '../BaseHelper/listing.helper';
import { NotificationHelper } from '../BaseHelper/notification.helper';

export class ChoiceTypeHelper extends ListingHelper {
    public formHelper: FormHelper;
    public notificationHelper: NotificationHelper;

    public choiceTypeSchema = {
        name: {
            type: 'text',
            required: true,
        },
        description: {
            type: 'textarea',
            required: true,
        },
    };

    constructor(page: Page) {
        super(page);
        this.formHelper = new FormHelper(page);
        this.notificationHelper = new NotificationHelper(page);
    }

    public async init() {
        await this.navigateTo('CHOICE_TYPES');
    }

    public async openChoiceTypeForm() {
        await this.clickButton('Add Choice Types');
    }

    public async fillChoiceTypeForm(data: ObjectDto) {
        await this.formHelper.fillFormInputInformation(
            this.choiceTypeSchema,
            data
        );
    }

    public async verifyChoiceTypeList(data: ObjectDto) {
        await this.searchInList(data.name);

        const row = await this.findRowInTable(data.name, 'NAME');
        await expect(await this.getCell(row, 'DESCRIPTION')).toContainText(
            data.description
        );
    }
}