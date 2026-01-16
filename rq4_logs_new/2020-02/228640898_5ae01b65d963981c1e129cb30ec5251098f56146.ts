import {MigrationInterface, QueryRunner} from "typeorm";

export class createFunctionCountTasks1581950972645 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('CREATE OR REPLACE FUNCTION public.count_tasks( id_user character varying) RETURNS integer LANGUAGE \'plpgsql\'  AS $BODY$ DECLARE count_task INTEGER; BEGIN select count(task.id) into count_task from "task" where task."missionId" in (select mission.id from "mission" where mission."userId" = id_user); RETURN count_task; END $BODY$;');
        await queryRunner.query('CREATE OR REPLACE FUNCTION count_finished_tasks( id_user character varying) RETURNS integer LANGUAGE \'plpgsql\'  AS $BODY$ DECLARE count_task INTEGER; BEGIN select count(task.id) into count_task from "task" where task.result = 1 AND task."missionId" in (select mission.id from "mission" where mission."userId" = id_user); RETURN count_task; END $BODY$;');
        await queryRunner.query('CREATE OR REPLACE FUNCTION public.count_missions( id_user character varying) RETURNS integer LANGUAGE \'plpgsql\'  AS $BODY$ DECLARE count_missions INTEGER; BEGIN select count(mission.id) into count_missions from "mission" where mission."userId" = id_user; RETURN count_missions; END $BODY$;');
    }

    public async down(queryRunner: QueryRunner): Promise<any> {
        await queryRunner.query('DROP FUNCTION count_task();');
        await queryRunner.query('DROP FUNCTION count_finished_tasks();');
        await queryRunner.query('DROP FUNCTION count_missions();');
    }

}