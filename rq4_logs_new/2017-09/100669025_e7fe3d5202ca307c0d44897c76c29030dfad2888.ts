import * as uuid from 'uuid';
import * as dynogels from 'drandx-dynogels';
import { BaseModel } from './BaseModel';
import { globalConst } from '../config/db/appVariables';
import { LOGIN_SESSION_STATUS } from './Enums';
import * as joi from 'joi';

export class UserPharmacy extends BaseModel {
    public userId: string;
    public pharmacyId: string;

    constructor() {
        super();
    }

    public model: dynogels.Model = dynogels.define(`${globalConst.stage}_user_pharmacies`, {
        hashKey: 'userId',
        rangeKey: 'pharmacyId',
        timestamps: false,
        schema: {
          userId: joi.string(),            
          pharmacyId: joi.string(),
          createdAt: joi.number(),
          updatedAt: joi.number(),
        },
        tableName: `${globalConst.stage}_user_pharmacies`,
    });
}