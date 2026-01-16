import Price from "./Price";

export default class Dormitory {
    private _id: number;
    private _number: number;
    private _pricePerMonth: Price;

    constructor(id: number, number: number, pricePerMonth: Price) {
        this._id = id;
        this._number = number;
        this._pricePerMonth = pricePerMonth;
    }

    get id(): number {
        return this._id;
    }

    set id(value: number) {
        this._id = value;
    }

    get number(): number {
        return this._number;
    }

    set number(value: number) {
        this._number = value;
    }

    get pricePerMonth(): Price {
        return this._pricePerMonth;
    }

    set pricePerMonth(value: Price) {
        this._pricePerMonth = value;
    }

}