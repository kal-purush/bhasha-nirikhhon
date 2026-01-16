import { Inject, Injectable } from '@nestjs/common';
import { AppError } from '../../../common/errors/Error';
import { InfluencerRepository } from '../repository/influencer.repository';
import { IInfluencerRepository } from '../interfaces/repository.interface';
import { Influencer } from '@prisma/client';

@Injectable()
export class DeleteInfluencerService {
  constructor(
    @Inject(InfluencerRepository)
    private readonly influencerRepository: IInfluencerRepository<Influencer>,
  ) {}

  async execute(id: string): Promise<void> {
    try {
      await this.influencerRepository.deleteInfluencer(id);
    } catch (error) {
      throw new AppError(
        'influencer-service.deleteInfluencer',
        500,
        'failed to delete influencer',
      );
    }
  }
}