import { Test } from '@nestjs/testing';
import { MockedTimelineRepository } from '../repository/mocked-timeline.repository';
import { TimelineReaderUseCase } from './timeline-reader.use-case';
import { InjectionToken } from '@/common/constant/injection-token.constant';

describe('TimelineReaderUseCase', () => {
  let mockedTimelineRepository: MockedTimelineRepository;
  let timelineReaderUseCase: TimelineReaderUseCase;

  beforeEach(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [{ provide: InjectionToken.TIMELINE_REPOSITORY, useClass: MockedTimelineRepository }, TimelineReaderUseCase],
    }).compile();

    mockedTimelineRepository = moduleRef.get(InjectionToken.TIMELINE_REPOSITORY);
    timelineReaderUseCase = moduleRef.get(TimelineReaderUseCase);
  });

  test('findTimelines', async () => {
    const expectTimeline = await mockedTimelineRepository.findMany();

    const foundTimelines = await timelineReaderUseCase.findTimelines({});

    expect(foundTimelines).toEqual(expectTimeline);
  });
});