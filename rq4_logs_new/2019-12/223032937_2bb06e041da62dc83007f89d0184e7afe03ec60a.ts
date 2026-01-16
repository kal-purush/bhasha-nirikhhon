import {StringUtils} from "../../util/StringUtils";

export default class CardDetails {

    private _cardNumber: string;
    private _ownerName: string;
    private _expirationDate: string;
    private _cvc: string;

    constructor(cardNumber: string, ownerName: string, expirationDate: string, cvc: string) {
        this._cardNumber = cardNumber;
        this._ownerName = ownerName;
        this._expirationDate = expirationDate;
        this._cvc = cvc;
    }

    static emptyData(): CardDetails {
        return new CardDetails('', '', '', '');
    }

    isDataFilled(): boolean {
        return this.isFieldFullFilled(this._cardNumber, 16)
            && StringUtils.isNotEmpty(this._ownerName)
            && this.isFieldFullFilled(this._expirationDate, 4)
            && this.isFieldFullFilled(this._cvc, 3);
    }

    private isFieldFullFilled(value: string, length: number): boolean {
        return value.length == length;
    }

    get cardNumber(): string {
        return this._cardNumber;
    }

    set cardNumber(value: string) {
        this._cardNumber = value;
    }

    get ownerName(): string {
        return this._ownerName;
    }

    set ownerName(value: string) {
        this._ownerName = value;
    }

    get expirationDate(): string {
        return this._expirationDate;
    }

    set expirationDate(value: string) {
        this._expirationDate = value;
    }

    get cvc(): string {
        return this._cvc;
    }

    set cvc(value: string) {
        this._cvc = value;
    }
}