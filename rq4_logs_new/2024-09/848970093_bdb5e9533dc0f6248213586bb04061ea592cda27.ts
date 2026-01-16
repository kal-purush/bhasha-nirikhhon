import { Test, TestingModule } from '@nestjs/testing';
import { BrandRepository } from '../repository/brand.repository';
import { PrismaService } from '../../../prisma.service';

describe('AdminRepository', () => {
  let brandRepository: BrandRepository;
  let prismaService: PrismaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [BrandRepository, PrismaService],
    }).compile();

    brandRepository = module.get<BrandRepository>(BrandRepository);
    prismaService = module.get<PrismaService>(PrismaService);
  });

  it('should be defined', () => {
    expect(brandRepository).toBeDefined();
    expect(prismaService).toBeDefined();
  });
});