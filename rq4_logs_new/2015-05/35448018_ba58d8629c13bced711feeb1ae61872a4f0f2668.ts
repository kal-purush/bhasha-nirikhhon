import { validatorFactory, IEmailConfig, EmailValidator } from '../main';

export let email = function (config: IEmailConfig) {
    return validatorFactory(new EmailValidator(config));
};