import { Inject, Injectable } from '@nestjs/common';
import { AppError } from '../../../common/errors/Error';
import { IBrandRepository } from '../interfaces/repository.interface';
import { Brand } from '@prisma/client';
import { BrandRepository } from '../repository/brand.repository';

@Injectable()
export class DeleteBrandService {
  constructor(
    @Inject(BrandRepository)
    private readonly brandRepository: IBrandRepository<Brand>,
  ) {}

  async execute(brandId: string): Promise<void> {
    try {
      return await this.brandRepository.deleteBrand(brandId);
    } catch (error) {
      throw new AppError(
        'brand-service.deleteBrand',
        500,
        'failed to delete brand',
      );
    }
  }
}