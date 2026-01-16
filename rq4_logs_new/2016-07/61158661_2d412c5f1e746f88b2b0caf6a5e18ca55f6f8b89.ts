import { injectable } from 'inversify';

import { IBrandRepository } from './_namespace';
import { BrandModel } from '../models/_namespace';

@injectable()
export class BrandRepository implements IBrandRepository {

    private _brands: Array<BrandModel>;

    constructor () {
        this._brands = [];

        this._brands.push(<BrandModel> {
            id: 1,
            name: 'Pot Noodle'
        });

        this._brands.push(<BrandModel> {
            id: 2,
            name: 'James Richford Food Co.'
        });
    }

    public getById(id: number): BrandModel {
        return this._brands.filter(
            u => u.id === id
        )[0];
    }

}