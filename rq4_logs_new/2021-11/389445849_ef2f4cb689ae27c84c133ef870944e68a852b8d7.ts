import { AccountMock } from "@/../tests/modules/account/mocks/account-mock";
import { connection } from "@/infra/database/postgres";
import { PostgresAddAccountRepository } from "@/infra/database/postgres/account/repositories";

const makeSut = () => {
  const sut = new PostgresAddAccountRepository();
  return {
    sut,
  };
};

const input = AccountMock();

describe("==> add_account_repository", () => {
  beforeAll(async () => {
    await connection.create();
    await connection.run_migrations();
  });

  afterEach(async () => {
    await connection.clear();
    await connection.run_migrations();
  });

  afterAll(async () => {
    await connection.clear();
    await connection.close();
  });

  it("should return a account on success", async () => {
    const { sut } = makeSut();

    const new_account = await sut.add(input);

    expect(new_account.name).toBe(input.name);
  });
});