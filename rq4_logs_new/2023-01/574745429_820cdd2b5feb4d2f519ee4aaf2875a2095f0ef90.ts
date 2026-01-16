import { ArgsType, Field, Int } from '@nestjs/graphql';
import { SkillOrderInput } from '../input/skill-order.input';
import { SkillWhereUniqueInput } from '../input/skill-where-unique.input';

@ArgsType()
export class FindSkillsArgs {
  @Field(() => [SkillOrderInput], { nullable: true })
  orderBy?: SkillOrderInput[];

  @Field(() => SkillWhereUniqueInput, { nullable: true })
  cursor?: SkillWhereUniqueInput;

  @Field(() => Int, { nullable: true })
  take?: number;

  @Field(() => Int, { nullable: true })
  skip?: number;
}