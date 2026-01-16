import { MigrationInterface, QueryRunner, Table, TableUnique } from 'typeorm';

export class AddAuthorityList1747150845945 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TYPE "authority_list_document_enum" AS ENUM('power-of-attorney-property')
    `);

    await queryRunner.createTable(
      new Table({
        name: 'authority_list',
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            generationStrategy: 'uuid',
            default: 'uuid_generate_v4()',
          },
          {
            name: 'document',
            type: 'authority_list_document_enum',
            isNullable: false,
          },
          {
            name: 'city',
            type: 'varchar',
            isNullable: false,
          },
          {
            name: 'authoritiesUk',
            type: 'text',
            isNullable: false,
          },
          {
            name: 'authoritiesEn',
            type: 'text',
            isNullable: false,
          },
          {
            name: 'createdAt',
            type: 'timestamp with time zone',
            default: 'now()',
          },
          {
            name: 'updatedAt',
            type: 'timestamp with time zone',
            default: 'now()',
          },
        ],
      }),
    );

    await queryRunner.createUniqueConstraint(
      'authority_list',
      new TableUnique({
        name: 'UQ_authority_list_city_document',
        columnNames: ['city', 'document'],
      }),
    );

    await queryRunner.query(`
      INSERT INTO "authority_list" ("id", "document", "city", "authoritiesUk", "authoritiesEn", "createdAt", "updatedAt")
VALUES 
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Львів',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Львівської міської ради, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Lviv City Council, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  ),
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Одеса',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Одеської міської ради, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Odesa City Council, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  ),
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Київ',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Київської міської державної адміністрації, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Kyiv City State Administration, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  ),
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Харків',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Харківської міської ради, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Kharkiv City Council, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  ),
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Дніпро',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Дніпровської міської ради, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Dnipro City Council, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  ),
  (
    uuid_generate_v4(),
    'power-of-attorney-property',
    'Запоріжжя',
    'органах державної влади, управліннях та департаментах місцевого самоврядування Запорізької міської ради, районних адміністраціях, їх структурних підрозділах, нотаріальних конторах, Укрдержреєстрі, БТІ, структурних підрозділах Міністерства юстиції України та інших органах виконавчої влади',
    'government authorities, departments and local self-government bodies of Zaporizhzhia City Council, district administrations, their departments, notary offices, Ukrderzhreestr, BTI, structural units of the Ministry of Justice of Ukraine and other executive authorities',
    now(), now()
  );
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropTable('authority_list');
    await queryRunner.query(`DROP TYPE "authority_list_document_enum"`);
  }
}