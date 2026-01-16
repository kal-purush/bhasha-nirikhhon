// import all interfaces
import { IWrite } from '@src/repositories/interfaces/IWrite';
import { IRead } from '@src/repositories/interfaces/IRead';
import { InternalError } from '@src/util/errors/internal-error';

// that class only can be extended
export abstract class BaseService<T> implements IWrite<T>, IRead<T> {
  async getAll(): Promise<T[]> {
    throw new Error('Method not implemented.');
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async create(item: T): Promise<boolean> {
    throw new Error('Method not implemented.');
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async update(id: string, item: T): Promise<boolean> {
    throw new Error('Method not implemented.');
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async delete(id: string): Promise<boolean> {
    throw new Error('Method not implemented.');
  }
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async getById(id: number): Promise<T> {
    throw new Error('Method not implemented.');
  }
}

export class TrackerServiceInternalError extends InternalError {
  constructor(message: string) {
    super(
      `Unexpected error during the service tracking processing: ${message}`
    );
  }
}