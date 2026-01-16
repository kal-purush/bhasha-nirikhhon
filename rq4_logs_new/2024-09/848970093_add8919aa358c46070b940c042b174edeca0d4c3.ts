import { Test, TestingModule } from '@nestjs/testing';
import { CreateBrandService } from '../services/create-brand.service';
import { FindAllBrandsService } from '../services/find-all-brands.service';
import { FindOneBrandService } from '../services/find-one-brand.service';
import { UpdateBrandService } from '../services/update-brand.service';
import { LinkInfluencerService } from '../services/link-influencer.service';
import { FindInfluencersByBrandService } from '../services/influencers-by-brand.service';
import { BrandRepository } from '../repository/brand.repository';

describe('AdminServices', () => {
  let createBrand: CreateBrandService;
  let findAllBrands: FindAllBrandsService;
  let findOneBrand: FindOneBrandService;
  let updateBrand: UpdateBrandService;
  let linkBrandInfluencer: LinkInfluencerService;
  let influencersByBrand: FindInfluencersByBrandService;

  let brandRepository: BrandRepository;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CreateBrandService,
        FindAllBrandsService,
        FindOneBrandService,
        UpdateBrandService,
        LinkInfluencerService,
        FindInfluencersByBrandService,
        {
          provide: BrandRepository,
          useValue: {
            createBrand: jest.fn().mockResolvedValue(null),
            findAllBrands: jest.fn().mockResolvedValue(null),
            findOneBrand: jest.fn().mockResolvedValue(null),
            updateBrand: jest.fn().mockResolvedValue(null),
            associateInfluencer: jest.fn().mockResolvedValue(null),
            findInfluencersByBrand: jest.fn().mockResolvedValue(null),
          },
        },
      ],
    }).compile();

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

    brandRepository = module.get<BrandRepository>(BrandRepository);
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  it('should be defined', () => {
    expect(createBrand).toBeDefined();
    expect(findAllBrands).toBeDefined();
    expect(findOneBrand).toBeDefined();
    expect(updateBrand).toBeDefined();
    expect(linkBrandInfluencer).toBeDefined();
    expect(influencersByBrand).toBeDefined();
  });
});