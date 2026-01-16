import { RegisterAccountController } from "@/modules/account/application/controllers/register-account/register-account-controller";
import { ValidatorAdapter } from "@/infra/email-validator/validator-adapter";
import { register_account_factory } from "../../use-cases/account";

export const register_account_controller_factory = () => {
  const emailValidator = new ValidatorAdapter();
  const registerAccount = register_account_factory();
  const registerAccountController = new RegisterAccountController(
    emailValidator,
    registerAccount
  );

  return registerAccountController;
};