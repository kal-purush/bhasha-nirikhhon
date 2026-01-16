import {InvoiceAppDto} from '^types/invoiceApp.type';

export type InvoiceAccountDto = {
    id?: number;
    organizationId?: number;
    image: string;
    email: string;
    invoiceApps: InvoiceAppDto[];
};