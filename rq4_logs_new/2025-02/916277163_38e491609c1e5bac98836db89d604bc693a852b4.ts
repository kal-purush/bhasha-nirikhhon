import { type INestBaseResponse, Nest } from '../../api';

import { QueryParameters } from '@repo/business/shared/interface';

import { Paginate } from '../../paginate';

import Supplier from './supplier';
import type { SupplierEntity } from './interface';

export class SupplierService {
  constructor(private nest: Nest) {}

  public async create(name: string, type: string): Promise<Supplier> {
    return await this.nest.finance.supplier
      .create({ name, type })
      .then((response) => new Supplier(response));
  }

  public async update(
    param: string,
    name: string,
    type?: string,
  ): Promise<Supplier> {
    return this.nest.finance.supplier
      .update(param, { name, type })
      .then((response) => new Supplier(response));
  }

  public async getAll(
    parameters: QueryParameters,
  ): Promise<Array<SupplierEntity> | Paginate<SupplierEntity>> {
    return await this.nest.finance.supplier.getAll(parameters);
  }

  public async get(param: string): Promise<Supplier> {
    return await this.nest.finance.supplier
      .getOne(param)
      .then((response) => new Supplier(response));
  }

  public async remove(param: string): Promise<INestBaseResponse> {
    return await this.nest.finance.supplier.delete(param);
  }
}