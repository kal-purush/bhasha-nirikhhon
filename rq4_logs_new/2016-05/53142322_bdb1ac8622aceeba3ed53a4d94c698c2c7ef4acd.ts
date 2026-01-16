import { Queryable, QueryableConstructor } from "./queryable";
import { Util } from "../../utils/util";
import { Item } from "./items";

export interface ODataParser<T> {
    parse(r: Response): Promise<T>;
}


export class ODataEntity<T extends Queryable> {

    public "odata.editLink": string;
    public "odata.etag": string;
    public "odata.id": string;
    public "odata.type": string;

    constructor(private factory: QueryableConstructor) {
    }

    public toQueryable(source: any): T {

        let n = new this.factory(this["odata.editLink"]);
        return n;
    }
}

export abstract class ODataParserBase<T extends Queryable> implements ODataParser<T> {
    constructor(private factory: QueryableConstructor) {
    }

    public parse(r: Response): Promise<T> {
        return r.json().then(function (json) {
            if (json.hasOwnProperty("d")) {
                if (json.d.hasOwnProperty("results")) {
                    return json.d.results;
                }

                return json.d;

            } else if (json.hasOwnProperty("value")) {

                return json.value;
            }

            return json;
        });
    }
}

export class ODataEntityParser<T extends Queryable> extends ODataParserBase<T> {

    constructor(factory: QueryableConstructor) {
        super(factory);
    }




}

class test extends Queryable {

    constructor(baseUrl: string | Queryable, path?: string) {
        super(baseUrl, path, new ODataEntityParser<Item>(Item));
    }



}

