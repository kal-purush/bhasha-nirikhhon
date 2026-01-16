import { inject, injectable } from "inversify";

import { NoodleModel } from "../models/_namespace";
import { NoodleDto } from "../dtos/_namespace";
import { INoodleRepository } from "../repositories/_namespace";
import { INoodleSerializer } from "../serializers/_namespace";
import { INoodleManager } from "./_namespace";

@injectable()
export class NoodleManager implements INoodleManager {

    private _noodleRepository: INoodleRepository;
    private _noodleSerializer: INoodleSerializer;

    constructor (
        @inject("INoodleRepository") reviewRepository: INoodleRepository,
        @inject("INoodleSerializer") reviewSerializer: INoodleSerializer
    ) {
        this._noodleRepository = reviewRepository;
        this._noodleSerializer = reviewSerializer;
    }

    public getById(id: number): NoodleDto {
        let model = this._noodleRepository.getById(id);

        return this._noodleSerializer.serialize(model);
    }

}