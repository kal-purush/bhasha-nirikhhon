import { Test, TestingModule } from '@nestjs/testing';
import { CreateInfluencerService } from '../services/create-influencer.service';
import { FindAllInfluencersService } from '../services/find-all-influencers.service';
import { FindOneInfluencerService } from '../services/find-one-influencer.service';
import { InfluencersByFilterService } from '../services/find-influencers-by-filter.service';
import { UpdateInfluencerService } from '../services/update-influencer.service';
import { S3BucketService } from '../../../common/aws/s3Bucket';
import { InfluencerRepository } from '../repository/influencer.repository';

describe('InfluencerServices', () => {
  let createInfluencer: CreateInfluencerService;
  let findAllInfluencers: FindAllInfluencersService;
  let findOneInfluencer: FindOneInfluencerService;
  let findInfluencerByFilter: InfluencersByFilterService;
  let updateInfluencer: UpdateInfluencerService;

  let s3Bucket: S3BucketService;
  let influencerRepository: InfluencerRepository;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CreateInfluencerService,
        FindAllInfluencersService,
        FindOneInfluencerService,
        InfluencersByFilterService,
        UpdateInfluencerService,
        {
          provide: S3BucketService,
          useValue: {
            uploadImage: jest.fn().mockResolvedValue('image_url'),
            deleteImage: jest.fn(),
          },
        },
        {
          provide: InfluencerRepository,
          useValue: {
            createInfluencer: jest.fn().mockResolvedValue(null),
            findAllInfluencers: jest.fn().mockResolvedValue(null),
            findInfluencerByFilter: jest.fn().mockResolvedValue(null),
            findOneInfluencer: jest.fn().mockResolvedValue(null),
            updateInfluencer: jest.fn().mockResolvedValue(null),
          },
        },
      ],
    }).compile();

    createInfluencer = module.get<CreateInfluencerService>(
      CreateInfluencerService,
    );
    findAllInfluencers = module.get<FindAllInfluencersService>(
      FindAllInfluencersService,
    );
    findOneInfluencer = module.get<FindOneInfluencerService>(
      FindOneInfluencerService,
    );
    findInfluencerByFilter = module.get<InfluencersByFilterService>(
      InfluencersByFilterService,
    );
    updateInfluencer = module.get<UpdateInfluencerService>(
      UpdateInfluencerService,
    );

    s3Bucket = module.get<S3BucketService>(S3BucketService);
    influencerRepository =
      module.get<InfluencerRepository>(InfluencerRepository);
  });

  it('should be defined', () => {
    expect(createInfluencer).toBeDefined();
    expect(findAllInfluencers).toBeDefined();
    expect(findOneInfluencer).toBeDefined();
    expect(findInfluencerByFilter).toBeDefined();
    expect(updateInfluencer).toBeDefined();
    expect(s3Bucket).toBeDefined();
  });
});