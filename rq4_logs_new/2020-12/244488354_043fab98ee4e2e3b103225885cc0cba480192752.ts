import { Approval } from '@interfaces/approval';
import { DataFile } from '@interfaces/data-file';
import { Event } from '@interfaces/event';
import { Hwm } from '@interfaces/hwm';
import { PeakSummary } from '@interfaces/peak-summary';
import { Site } from '@interfaces/site';
export interface Member {
    agency_id?: number;
    approvals?: Approval[];
    data_file?: DataFile[];
    email?: string;
    events?: Event[];
    fname?: string;
    hwms?: Hwm[];
    hwms1?: Hwm[];
    instrument_status?: [];
    lname?: string;
    member_id?: number;
    password?: string;
    peak_summary?: PeakSummary[];
    phone?: string;
    role_id?: number;
    salt?: string;
    sites?: Site[];
    username: string;
}