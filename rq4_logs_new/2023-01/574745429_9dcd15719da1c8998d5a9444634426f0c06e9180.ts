import { Module } from '@nestjs/common';
import { SkillQuery } from './controller/skill-query.resolver';
import { SkillRepository } from './repository/skill.repository';
import { SkillReaderUseCase } from './use-case/timeline-reader.use-case';
import { InjectionToken } from '@/common/constant/injection-token.constant';

@Module({
  providers: [
    { provide: InjectionToken.SKILL_REPOSITORY, useClass: SkillRepository },
    { provide: InjectionToken.SKILL_READER_USE_CASE, useClass: SkillReaderUseCase },
    SkillQuery,
  ],
  exports: [{ provide: InjectionToken.SKILL_REPOSITORY, useClass: SkillRepository }],
})
export class SkillModule {}