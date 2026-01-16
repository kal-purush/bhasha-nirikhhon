import { injectable } from "inversify";

import { INoodleRepository } from "./_namespace";
import { NoodleModel } from "../models/_namespace";

@injectable()
export class NoodleRepository implements INoodleRepository {

    private _noodles: Array<NoodleModel>;

    constructor () {
        this._noodles = [];

        this._noodles.push(<NoodleModel> {
            id: 1,
            name: "Pot Noodle"
        });

        this._noodles.push(<NoodleModel> {
            id: 2,
            name: "James Richford's Spicy Noodles"
        });
    }

    public getById(id: number): NoodleModel {
        return this._noodles.filter(
            u => u.id === id
        )[0];
    }

}