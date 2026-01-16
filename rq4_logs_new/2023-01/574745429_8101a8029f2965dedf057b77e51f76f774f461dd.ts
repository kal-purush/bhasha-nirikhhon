import { Test } from '@nestjs/testing';
import { MockedSkillRepository } from '../repository/mocked-skill.repository';
import { SkillReaderUseCase } from './timeline-reader.use-case';
import { InjectionToken } from '@/common/constant/injection-token.constant';

describe('SkillReaderUseCase', () => {
  let mockedSkillRepository: MockedSkillRepository;
  let timelineReaderUseCase: SkillReaderUseCase;

  beforeEach(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [{ provide: InjectionToken.SKILL_REPOSITORY, useClass: MockedSkillRepository }, SkillReaderUseCase],
    }).compile();

    mockedSkillRepository = moduleRef.get(InjectionToken.SKILL_REPOSITORY);
    timelineReaderUseCase = moduleRef.get(SkillReaderUseCase);
  });

  test('findSkills', async () => {
    const expectSkill = await mockedSkillRepository.findMany();

    const foundSkills = await timelineReaderUseCase.findSkills({});

    expect(foundSkills).toEqual(expectSkill);
  });
});