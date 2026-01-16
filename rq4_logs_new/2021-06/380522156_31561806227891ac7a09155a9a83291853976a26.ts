import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { PriceClose } from './price-close.entity';
import { PriceCloseService } from './price-close.service';

const priceCloseArray = [
  {
    date: new Date('2020-03-25'),
    company_id: '46B285BC-B25F-4814-985C-390A4BFA2023',
    price: 15.0,
    date_created: new Date('2020-03-25 01:01:16.7966667')
  },
  {
    date: new Date('2020-03-24'),
    company_id: '46B285BC-B25F-4814-985C-390A4BFA2023',
    price: 14.0,
    date_created: new Date('2020-03-25 01:01:16.7966667')
  },
  {
    date: new Date('2020-03-23'),
    company_id: '46B285BC-B25F-4814-985C-390A4BFA2023',
    price: 13.0,
    date_created: new Date('2020-03-25 01:01:16.7966667')
  }
];

const priceCloseWithVolatilityArray = [
  {
    date: new Date('2020-03-25'),
    company_id: '46B285BC-B25F-4814-985C-390A4BFA2023',
    price: 15.0,
    date_created: new Date('2020-03-25 01:01:16.7966667'),
    volatility: 2
  }
]

describe('PriceCloseService', () => {
  let service: PriceCloseService;
  let repo: Repository<PriceClose>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PriceCloseService,
        {
          provide: getRepositoryToken(PriceClose),
          useValue: {
            find: jest.fn().mockResolvedValue(priceCloseArray),
          },
        },
      ],
    }).compile();

    service = module.get<PriceCloseService>(PriceCloseService);
    repo = module.get<Repository<PriceClose>>(getRepositoryToken(PriceClose));
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('findLatestWithVolatility', () => {
    it('should return an array of priceCloseWithVolatility', async () => {
      const priceCloses = await service.findLatestWithVolatility();
      expect(priceCloses).toEqual(priceCloseWithVolatilityArray);
    });
  });
});