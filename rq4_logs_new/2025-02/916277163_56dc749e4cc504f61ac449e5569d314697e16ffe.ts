import { type INestBaseResponse, Nest } from '../../api';

import { QueryParameters } from '../../shared';
import { Paginate } from '../../paginate';

import SupplierType from './supplierType';
import type { SupplierTypeEntity } from './interface';

export class SupplierTypeService {
  constructor(private nest: Nest) {}

  public async create(name: string): Promise<SupplierType> {
    return await this.nest.finance.supplier.type
      .create({ name })
      .then((response) => new SupplierType(response));
  }

  public async update(param: string, name: string): Promise<SupplierType> {
    return await this.nest.finance.supplier.type
        .update(param, { name })
        .then((response) => new SupplierType(response));
  }

  public async getAll(
    parameters: QueryParameters,
  ): Promise<Array<SupplierTypeEntity> | Paginate<SupplierTypeEntity>> {
    return await this.nest.finance.supplier.type.getAll(parameters);
  }

  public async get(param: string): Promise<SupplierTypeEntity> {
    return await this.nest.finance.supplier.type.getOne(param);
  }

  public async remove(param: string): Promise<INestBaseResponse> {
    return await this.nest.finance.supplier.type.delete(param);
  }

}