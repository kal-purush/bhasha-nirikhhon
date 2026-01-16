import { AccountMock } from "@/../tests/modules/account/mocks/account-mock";
import { connection } from "@/infra/database/postgres";
import {
  PostgresCheckByEmailRepository,
  PostgresAddAccountRepository,
} from "@/infra/database/postgres/account/repositories";

const input = AccountMock();

const makeSut = () => {
  const sut = new PostgresCheckByEmailRepository();
  const create_account = new PostgresAddAccountRepository();
  return {
    sut,
    create_account,
  };
};

describe("==> check_by_email_repository", () => {
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

  it("should return a false if email is not in use", async () => {
    const { sut } = makeSut();

    const verify = await sut.ifAlreadyInUse("jhon_doe@email.com");

    expect(verify).toBe(false);
  });

  it("should return a true if email is already in use", async () => {
    const { sut, create_account } = makeSut();

    await create_account.add(input);
    const verify = await sut.ifAlreadyInUse(input.email);

    expect(verify).toBe(true);
  });
});