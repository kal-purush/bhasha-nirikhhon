import { Injectable } from '@nestjs/common';
import { Skill } from '../domain/model/skill.model';
import { SkillRepositoryInterface } from '../domain/service/repository/skill.repository';

@Injectable()
export class MockedSkillRepository implements SkillRepositoryInterface {
  async findMany() {
    const foundSkills = [
      {
        id: 'abc-123',
        name: 'hoge',
        description: 'hoge',
        officialLink: 'http://hoge.com',
        iconUrl: 'http://hoge.com/icon.png',
        createdAt: new Date('2021-01-01T00:00:00.000Z'),
      },
      {
        id: 'abc-456',
        name: 'fuga',
        description: 'fuga',
        officialLink: 'http://fuga.com',
        iconUrl: 'http://fuga.com/icon.png',
        createdAt: new Date('2021-01-01T00:00:00.000Z'),
      },
    ];

    return foundSkills.map((foundSkill) => new Skill(foundSkill));
  }
}