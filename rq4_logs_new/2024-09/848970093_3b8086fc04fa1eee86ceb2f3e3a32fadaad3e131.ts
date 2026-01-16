import { Test, TestingModule } from '@nestjs/testing';
import { InfluencerRepository } from '../repository/influencer.repository';
import { PrismaService } from '../../../prisma.service';

describe('AdminRepository', () => {
  let influencerRepository: InfluencerRepository;
  let prismaService: PrismaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [InfluencerRepository, PrismaService],
    }).compile();

    influencerRepository =
      module.get<InfluencerRepository>(InfluencerRepository);
    prismaService = module.get<PrismaService>(PrismaService);
  });

  it('should be defined', () => {
    expect(influencerRepository).toBeDefined();
    expect(prismaService).toBeDefined();
  });
});