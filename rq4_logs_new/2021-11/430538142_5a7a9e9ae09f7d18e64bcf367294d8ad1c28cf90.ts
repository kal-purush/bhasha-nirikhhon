import { Test, TestingModule } from '@nestjs/testing';
import { AuthQueriesResolver } from './auth-queries.resolver';

describe('AuthQueriesResolver', () => {
  let resolver: AuthQueriesResolver;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AuthQueriesResolver],
    }).compile();

    resolver = module.get<AuthQueriesResolver>(AuthQueriesResolver);
  });

  it('should be defined', () => {
    expect(resolver).toBeDefined();
  });
});