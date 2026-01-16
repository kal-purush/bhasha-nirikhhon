import * as _ from 'lodash';
export class EmploymentState {
    guid: string;
    contactGuid: string;
    companyRoleGuid: string;
    startDate: string;
    endDate: string;
}

export class Employment {
    private _state: EmploymentState;
    constructor(state?: EmploymentState) {
        this._state = state;
        if (!state) {
            this._state = new EmploymentState();
        }
    }

    get guid(): string { return this._state.guid; };
    set guid(value: string) { this._state.guid = value; };

    get contactGuid(): string { return this._state.contactGuid; };
    set contactGuid(value: string) { this._state.contactGuid = value; };

    get companyRoleGuid(): string { return this._state.companyRoleGuid; };
    set companyRoleGuid(value: string) { this._state.companyRoleGuid = value; };

    get startDate(): string { return this._state.startDate; };
    set startDate(value: string) { this._state.startDate = value; };

    get endDate(): string { return this._state.endDate; };
    set endDate(value: string) { this._state.endDate = value; };

    public clone(): Employment {
        return new Employment(_.clone(this._state));
    }
}