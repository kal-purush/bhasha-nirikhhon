import { Module } from '@nestjs/common';
import { TimelineQuery } from './controller/timeline-query.resolver';
import { TimelineRepository } from './repository/timeline.repository';
import { TimelineReaderUseCase } from './use-case/timeline-reader.use-case';
import { InjectionToken } from '@/common/constant/injection-token.constant';

@Module({
  providers: [
    { provide: InjectionToken.TIMELINE_REPOSITORY, useClass: TimelineRepository },
    { provide: InjectionToken.TIMELINE_READER_USE_CASE, useClass: TimelineReaderUseCase },
    TimelineQuery,
  ],
  exports: [{ provide: InjectionToken.TIMELINE_REPOSITORY, useClass: TimelineRepository }],
})
export class TimelineModule {}