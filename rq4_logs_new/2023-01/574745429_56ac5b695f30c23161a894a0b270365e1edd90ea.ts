import { Injectable } from '@nestjs/common';
import { Timeline } from '../domain/model/timeline.model';
import { TimelineRepositoryInterface } from '../domain/service/repository/timeline.repository';

@Injectable()
export class MockedTimelineRepository implements TimelineRepositoryInterface {
  async findMany() {
    const foundTimelines = [
      {
        id: 'abc-123',
        title: 'hoge',
        happenedAt: new Date('2021-01-01T00:00:00.000Z'),
        createdAt: new Date('2021-01-01T00:00:00.000Z'),
      },
      {
        id: 'abc-456',
        title: 'hoge',
        happenedAt: new Date('2021-01-01T00:00:00.000Z'),
        createdAt: new Date('2021-01-01T00:00:00.000Z'),
      },
    ];

    return foundTimelines.map((foundTimeline) => new Timeline(foundTimeline));
  }
}