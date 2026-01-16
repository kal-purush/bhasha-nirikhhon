import { inject, injectable } from 'tsyringe';

import Provider from '../infra/typeorm/entities/Provider';
import IProviderRepository from '../repositories/IProviderRepository';

@injectable()
export default class GetTop5ProvidersService {
  constructor(
    @inject('ProviderRepository')
    private providerRepository: IProviderRepository
  ) {}

  public async execute(): Promise<Provider[] | undefined> {
    const providers = await this.providerRepository.findTop5Providers();
    return providers;
  }
}