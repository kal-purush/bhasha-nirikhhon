import { Inject, Logger } from '@nestjs/common';
import { Args, Query, Resolver } from '@nestjs/graphql';
import { Skill as SkillModel } from '../domain/model/skill.model';
import { SkillReaderUseCaseInterface } from '../domain/service/use-case/skill-reader.use-case';
import { FindSkillsArgs } from './dto/args/find-skills.args';
import { Skill } from './dto/object/skill.object';
import { InjectionToken } from '@/common/constant/injection-token.constant';

@Resolver()
export class SkillQuery {
  private readonly logger = new Logger(SkillQuery.name);

  constructor(
    @Inject(InjectionToken.SKILL_READER_USE_CASE)
    private readonly SkillReaderUseCase: SkillReaderUseCaseInterface,
  ) {}

  @Query(() => [Skill])
  async findSkills(@Args() args: FindSkillsArgs): Promise<SkillModel[]> {
    this.logger.log('findSkills called');
    this.logger.log(args);

    const foundSkills = await this.SkillReaderUseCase.findSkills({
      orderBy: args.orderBy,
      cursor: args.cursor,
      take: args.take,
      skip: args.skip,
    });

    return foundSkills;
  }
}