export class Product {
    //ProductDTO in POS

    private _code: string;
    private _description: string;

    constructor(private aCode: string, private aDescription: string) {

        /*this._code = aCode;
        this._description = aDescription;*/
    }

    get code(): string {
        return this._code;
    }

    get description(): string {
        return this._description;
    }

    set code(aCode: string) {
        this._code = aCode;
    }

    set description(aDescription: string) {
        this._code = aDescription;
    }



}