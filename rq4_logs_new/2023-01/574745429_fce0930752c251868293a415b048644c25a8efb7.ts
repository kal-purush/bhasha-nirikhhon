import { Inject, Logger } from '@nestjs/common';
import { Args, Query, Resolver } from '@nestjs/graphql';
import { Timeline as TimelineModel } from '../domain/model/timeline.model';
import { TimelineReaderUseCaseInterface } from '../domain/service/use-case/timeline-reader.use-case';
import { FindTimelinesArgs } from './dto/args/find-timelines.args';
import { Timeline } from './dto/object/timeline.object';
import { InjectionToken } from '@/common/constant/injection-token.constant';

@Resolver()
export class TimelineQuery {
  private readonly logger = new Logger(TimelineQuery.name);

  constructor(
    @Inject(InjectionToken.TIMELINE_READER_USE_CASE)
    private readonly TimelineReaderUseCase: TimelineReaderUseCaseInterface,
  ) {}

  @Query(() => [Timeline])
  async findTimelines(@Args() args: FindTimelinesArgs): Promise<TimelineModel[]> {
    this.logger.log('findTimelines called');
    this.logger.log(args);

    const foundTimelines = await this.TimelineReaderUseCase.findTimelines({
      orderBy: args.orderBy,
      cursor: args.cursor,
      take: args.take,
      skip: args.skip,
    });

    return foundTimelines;
  }
}