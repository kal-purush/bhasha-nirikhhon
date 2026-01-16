import * as dynogels from 'drandx-dynogels';
import * as uuid from 'uuid';
import * as joi from 'joi';
import { BaseModel } from './BaseModel';
import { STATUS_ENUM } from './Enums';
import { awsConfig, globalConst } from '../config/db/appVariables';

dynogels.AWS.config.update(awsConfig);

export class Specialty extends BaseModel {
  public id: string;
  public name: string;
  public status: STATUS_ENUM;
  

  constructor() {
    super();
    this.id = uuid.v4();
    this.status = STATUS_ENUM.ACTIVE;
  }

  public model: dynogels.Model = dynogels.define(`${globalConst.stage}_specialties`, {
    hashKey: 'id',
    timestamps: false,
    schema: {
      id: joi.string(),
      name: joi.string(),
      status: joi.string(),
      createdAt: joi.number(),
      updatedAt: joi.number(),
    },
    tableName: `${globalConst.stage}_specialties`,
    indexes: [
      { hashKey: 'status', rangeKey: 'name', type: 'global', name: 'statusIndex',},
      ]
});
}