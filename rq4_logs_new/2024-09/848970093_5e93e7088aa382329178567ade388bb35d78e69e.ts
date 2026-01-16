import { Test, TestingModule } from '@nestjs/testing';
import { BrandController } from '../brand.controller';
import { CreateBrandService } from '../services/create-brand.service';
import { FindAllBrandsService } from '../services/find-all-brands.service';
import { FindOneBrandService } from '../services/find-one-brand.service';
import { UpdateBrandService } from '../services/update-brand.service';
import { LinkInfluencerService } from '../services/link-influencer.service';
import { FindInfluencersByBrandService } from '../services/influencers-by-brand.service';

describe('InfluencerController', () => {
  let controller: BrandController;

  let createBrand: CreateBrandService;
  let findAllBrands: FindAllBrandsService;
  let findOneBrand: FindOneBrandService;
  let updateBrand: UpdateBrandService;
  let linkBrandInfluencer: LinkInfluencerService;
  let influencersByBrand: FindInfluencersByBrandService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [BrandController],
      providers: [
        {
          provide: CreateBrandService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: FindAllBrandsService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: FindOneBrandService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: UpdateBrandService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: LinkInfluencerService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: FindInfluencersByBrandService,
          useValue: {
            execute: jest.fn().mockResolvedValue(null),
          },
        },
      ],
    }).compile();

    controller = module.get<BrandController>(BrandController);

    createBrand = module.get<CreateBrandService>(CreateBrandService);
    findAllBrands = module.get<FindAllBrandsService>(FindAllBrandsService);
    findOneBrand = module.get<FindOneBrandService>(FindOneBrandService);
    updateBrand = module.get<UpdateBrandService>(UpdateBrandService);
    linkBrandInfluencer = module.get<LinkInfluencerService>(
      LinkInfluencerService,
    );
    influencersByBrand = module.get<FindInfluencersByBrandService>(
      FindInfluencersByBrandService,
    );
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});