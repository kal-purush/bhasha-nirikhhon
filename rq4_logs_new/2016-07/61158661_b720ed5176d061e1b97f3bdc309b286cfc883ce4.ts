import { inject, injectable } from 'inversify';

import { BrandDto } from '../dtos/_namespace';
import { IBrandRepository } from '../repositories/_namespace';
import { IBrandSerializer } from '../serializers/_namespace';
import { IBrandManager } from './_namespace';

@injectable()
export class BrandManager implements IBrandManager {

    private _brandRepository: IBrandRepository;
    private _brandSerializer: IBrandSerializer;

    constructor (
        @inject('IBrandRepository') reviewRepository: IBrandRepository,
        @inject('IBrandSerializer') reviewSerializer: IBrandSerializer
    ) {
        this._brandRepository = reviewRepository;
        this._brandSerializer = reviewSerializer;
    }

    public getById(id: number): BrandDto {
        let model = this._brandRepository.getById(id);

        return this._brandSerializer.serialize(model);
    }

}