import { ArgsType, Field, Int } from '@nestjs/graphql';
import { TimelineOrderInput } from '../input/timeline-order.input';
import { TimelineWhereUniqueInput } from '../input/timeline-where-unique.input';

@ArgsType()
export class FindTimelinesArgs {
  @Field(() => [TimelineOrderInput], { nullable: true })
  orderBy?: TimelineOrderInput[];

  @Field(() => TimelineWhereUniqueInput, { nullable: true })
  cursor?: TimelineWhereUniqueInput;

  @Field(() => Int, { nullable: true })
  take?: number;

  @Field(() => Int, { nullable: true })
  skip?: number;
}