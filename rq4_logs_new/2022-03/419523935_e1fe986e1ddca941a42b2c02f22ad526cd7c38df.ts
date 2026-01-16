import { inject, injectable } from 'tsyringe';

import Service from '@modules/services/infra/typeorm/entities/Service';
import IServiceRepository from '../repositories/iServiceRepository';

@injectable()
export default class GetAllServicesManager{
  constructor(
    @inject('ServiceRepository')
    private serviceRepository : IServiceRepository
  ) {}

  public async execute(): Promise<Service[]> {
    const services = await this.serviceRepository.findAll();

    if (!services || services.length == 0) {
      throw new Error ('services not found');
    }

    return services;
  }
}