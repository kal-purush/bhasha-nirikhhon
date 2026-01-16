import { Inject, Injectable } from '@nestjs/common';
import { AppError } from '../../../common/errors/Error';
import { InfluencerRepository } from '../repository/influencer.repository';
import { IInfluencerRepository } from '../interfaces/repository.interface';
import { Influencer } from '@prisma/client';
import {
  IInfluencerDetails,
  IInfluencerFilters,
} from '../interfaces/influencer.interface';

@Injectable()
export class InfluencersByFilterService {
  constructor(
    @Inject(InfluencerRepository)
    private readonly influencerRepository: IInfluencerRepository<Influencer>,
  ) {}

  private transformInfluencerData(influencer: any): IInfluencerDetails {
    return {
      id: influencer.id,
      name: influencer.name,
      username: influencer.username,
      reach: influencer.reach,
      photo: influencer.photo,
      niches: influencer.Niche.map((nicheObj) => nicheObj.niche.name),
      address: {
        zipCode: influencer.InfluencerAddress[0]?.zipCode,
        state: influencer.InfluencerAddress[0]?.state,
        city: influencer.InfluencerAddress[0]?.city,
        street: influencer.InfluencerAddress[0]?.street,
        number: influencer.InfluencerAddress[0]?.number,
      },
      createdAt: influencer.created_at,
      updatedAt: influencer.updated_at,
    };
  }

  async execute(filters: IInfluencerFilters): Promise<IInfluencerDetails[]> {
    try {
      const influencersData =
        await this.influencerRepository.findInfluencerByFilter(filters);

      return influencersData.map((influencer) =>
        this.transformInfluencerData(influencer),
      );
    } catch (error) {
      throw new AppError(
        'influencer-service.findInfluencersByFilter',
        500,
        'failed to get influencers',
      );
    }
  }
}