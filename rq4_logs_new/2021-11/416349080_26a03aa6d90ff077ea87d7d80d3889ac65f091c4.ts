import { User } from "../../../entities/user.orm.entity";
import { firstUsers } from "./seeds/users.seed";

import { MigrationInterface, QueryRunner, getRepository } from "typeorm";

export class SeedUsers1634746230799 implements MigrationInterface {
  public async up(_queryRunner: QueryRunner): Promise<void> {
    firstUsers.forEach(async (user: Partial<User>) => {
      const result = await getRepository("user").save(user);
      console.log(result);
    });
  }

  public async down(_queryRunner: QueryRunner): Promise<void> {
    console.log(`Nothing to run o migration down`);
  }
}