import { AddAccountRepositoryContract } from "@/modules/account/domain/contracts/repositories";
import { Repository, getRepository } from "typeorm";
import { PostgresAccoutnEntity } from "../entities/account-entity";

export class PostgresAddAccountRepository
  implements AddAccountRepositoryContract
{
  private ormconfig: Repository<PostgresAccoutnEntity>;

  constructor() {
    this.ormconfig = getRepository(PostgresAccoutnEntity);
  }

  public async add(
    input: AddAccountRepositoryContract.Input
  ): Promise<AddAccountRepositoryContract.Output> {
    const account = this.ormconfig.create(input);

    await this.ormconfig.save(account);

    return account;
  }
}