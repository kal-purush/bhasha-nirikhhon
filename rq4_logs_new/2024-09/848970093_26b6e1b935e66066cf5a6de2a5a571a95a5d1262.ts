import { Inject, Injectable } from '@nestjs/common';
import { AppError } from '../../../common/errors/Error';
import { IBrandRepository } from '../interfaces/repository.interface';
import { Brand } from '@prisma/client';
import { BrandRepository } from '../repository/brand.repository';

@Injectable()
export class UnlinkInfluencerService {
  constructor(
    @Inject(BrandRepository)
    private readonly brandRepository: IBrandRepository<Brand>,
  ) {}

  async execute(brandId: string, influencerId: string): Promise<void> {
    try {
      return await this.brandRepository.disassociateInfluencer(
        brandId,
        influencerId,
      );
    } catch (error) {
      throw new AppError(
        'brand-service.unlinkInfluencer',
        500,
        'failed to unlink influencer from brand',
      );
    }
  }
}