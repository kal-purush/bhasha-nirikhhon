export class ValidateDTO {

    private _failMessage: string;
    private _success: boolean;

    constructor(failMessage: string, success: boolean) {
        this._failMessage = failMessage;
        this._success = success;
    }

    get success(): boolean {
        return this._success;
    }

    set success(value: boolean) {
        this._success = value;
    }

    get failMessage(): string {
        return this._failMessage;
    }

    set failMessage(value: string) {
        this._failMessage = value;
    }
}