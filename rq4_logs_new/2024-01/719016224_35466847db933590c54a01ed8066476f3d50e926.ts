import { UserMajor } from '@src/entities/Major';
import {
  generateCreatedAtColumn,
  generatePrimaryColumn,
  generateUpdatedAtColumn,
} from 'migrations/__utils/util';
import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class UserMajor1706435061683 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // 전공
    await queryRunner.createTable(
      new Table({
        name: 'user_major',
        columns: [
          generatePrimaryColumn('전공 고유 ID'),
          {
            name: 'code',
            type: 'varchar',
            length: '20',
            isNullable: false,
            comment: '전공 코드',
          },
          {
            name: 'name',
            type: 'varchar',
            length: '50',
            isNullable: false,
            comment: '전공 명',
          },
          {
            name: 'memo',
            type: 'varchar',
            length: '255',
            isNullable: false,
            comment: '메모',
          },
          generateCreatedAtColumn(),
          generateUpdatedAtColumn(),
        ],
      }),
    );
    await queryRunner.query('ALTER TABLE user_major COMMENT = "전공"');

    await queryRunner.manager.getRepository(UserMajor).upsert(
      [
        {
          code: '01',
          name: '디지털산업디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '02',
          name: '시각디자인과',
          memo: 'init seeding data',
        },
        {
          code: '03',
          name: '건축학과',
          memo: 'init seeding data',
        },
        {
          code: '04',
          name: '주얼리디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '05',
          name: '기계설계학과',
          memo: 'init seeding data',
        },
        {
          code: '06',
          name: '멀티미디어디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '07',
          name: '기계자동차학과',
          memo: 'init seeding data',
        },
        {
          code: '08',
          name: '컴퓨터전자공학과',
          memo: 'init seeding data',
        },
        {
          code: '09',
          name: '토목공학과',
          memo: 'init seeding data',
        },
        {
          code: '10',
          name: '산업경영공학과',
          memo: 'init seeding data',
        },
        {
          code: '11',
          name: '비즈니스영어과',
          memo: 'init seeding data',
        },
        {
          code: '12',
          name: '컴퓨터소프트웨어학과',
          memo: 'init seeding data',
        },
        {
          code: '13',
          name: '실내건축과',
          memo: 'init seeding data',
        },
        {
          code: '14',
          name: '방송영상미디어학과',
          memo: 'init seeding data',
        },
        {
          code: '15',
          name: '메카트로닉스공학과',
          memo: 'init seeding data',
        },
        {
          code: '16',
          name: '정보통신공학과',
          memo: 'init seeding data',
        },
        {
          code: '17',
          name: '비서학과',
          memo: 'init seeding data',
        },
        {
          code: '18',
          name: '건설안전공학과',
          memo: 'init seeding data',
        },
        {
          code: '19',
          name: '리빙세라믹디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '20',
          name: '웹툰만화창작학과',
          memo: 'init seeding data',
        },
        {
          code: '21',
          name: '방송연예과',
          memo: 'init seeding data',
        },
        {
          code: '22',
          name: '관광서비스경영학과',
          memo: 'init seeding data',
        },
        {
          code: '23',
          name: '중국어과',
          memo: 'init seeding data',
        },
        {
          code: '25',
          name: '도시디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '26',
          name: '비즈니스일본어과',
          memo: 'init seeding data',
        },
        {
          code: '27',
          name: '사회복지학과',
          memo: 'init seeding data',
        },
        {
          code: '28',
          name: '세무회계학과',
          memo: 'init seeding data',
        },
        {
          code: '29',
          name: '방송연예과(방송연기전공)',
          memo: 'init seeding data',
        },
        {
          code: '30',
          name: '방송연예과(방송분장전공)',
          memo: 'init seeding data',
        },
        {
          code: '31',
          name: '중국어과(cs-중국어서비스전공)',
          memo: 'init seeding data',
        },
        {
          code: '32',
          name: '중국어과(비즈니스)',
          memo: 'init seeding data',
        },
        {
          code: '33',
          name: '사회복지과(사회복지전공)',
          memo: 'init seeding data',
        },
        {
          code: '34',
          name: '사회복지과(아동보육전공)',
          memo: 'init seeding data',
        },
        {
          code: '35',
          name: '융합기계공학과',
          memo: 'init seeding data',
        },
        {
          code: '36',
          name: '게임&VR디자인학과',
          memo: 'init seeding data',
        },
        {
          code: '37',
          name: '방송뷰티메이크업과',
          memo: 'init seeding data',
        },
        {
          code: '38',
          name: '방송연예과(뮤지컬전공)',
          memo: 'init seeding data',
        },
        {
          code: '39',
          name: '방송연예과(K-POP전공)',
          memo: 'init seeding data',
        },
        {
          code: '40',
          name: '방송뷰티학과(메이크업헤어전공)',
          memo: 'init seeding data',
        },
        {
          code: '41',
          name: '방송뷰티학과(스타일리스트전공)',
          memo: 'init seeding data',
        },
        {
          code: '42',
          name: '글로벌항공서비스학과',
          memo: 'init seeding data',
        },
        {
          code: '43',
          name: '휴먼사회복지학과',
          memo: 'init seeding data',
        },
        {
          code: '44',
          name: '방송연예과(연기전공)',
          memo: 'init seeding data',
        },
        {
          code: '45',
          name: '방송연예과',
          memo: 'init seeding data',
        },
        {
          code: '62',
          name: '친환경자율주행자동차학과',
          memo: 'init seeding data',
        },
        {
          code: '63',
          name: '3D모델리버스엔지니어학과',
          memo: 'init seeding data',
        },
      ],
      ['name'],
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable(new Table({ name: 'user_major' }));
  }
}