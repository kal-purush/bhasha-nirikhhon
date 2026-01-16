import { BcryptAdapter } from "@/infra/hash/bcrypt-adapter";
import { NodemailerAdapter } from "@/infra/smtp/nodemailer-adapter";
import { RegisterAccount } from "@/modules/account/domain/use-cases/register-account";
import {
  PostgresAddAccountRepository,
  PostgresCheckByEmailRepository,
} from "@/infra/database/postgres/account/repositories";

export const register_account_factory = () => {
  const hashAdapter = new BcryptAdapter();
  const smtpAdapter = new NodemailerAdapter();
  const addAccountRepository = new PostgresAddAccountRepository();
  const checkByEmailRepository = new PostgresCheckByEmailRepository();
  const registerAccount = new RegisterAccount(
    checkByEmailRepository,
    hashAdapter,
    addAccountRepository,
    smtpAdapter
  );

  return registerAccount;
};