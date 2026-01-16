import { Test, TestingModule } from '@nestjs/testing';
import { WorkInterruptionResolver } from './work-interruption.resolver';
import { WorkInterruptionService } from './work-interruption.service';

describe('WorkInterruptionResolver', () => {
  let resolver: WorkInterruptionResolver;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [WorkInterruptionResolver, WorkInterruptionService],
    }).compile();

    resolver = module.get<WorkInterruptionResolver>(WorkInterruptionResolver);
  });

  it('should be defined', () => {
    expect(resolver).toBeDefined();
  });
});