import { faker } from '@faker-js/faker';
import { CreateBrandDto } from '../../dto/create-brand.dto';
import { Niche } from '../../../../modules/influencer/enums/niche.enum';
import { IBrand } from '../../interfaces/brand.interface';

export const MockCreateBrandDto: CreateBrandDto = {
  name: faker.company.name(),
  description: faker.commerce.productDescription(),
  niches: [faker.helpers.arrayElement(Object.values(Niche))],
};

export const MockIBrand: IBrand = {
  id: faker.number.int(),
  name: MockCreateBrandDto.name,
  description: MockCreateBrandDto.description,
  createdAt: new Date(),
  updatedAt: new Date(),
};