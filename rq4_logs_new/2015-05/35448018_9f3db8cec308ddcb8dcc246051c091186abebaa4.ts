import { BaseModel, IValidatorObject} from './main';

export let validatorFactory = function (validatorName: string, validatorMessage: string,  validatorObject: IValidatorObject) {
    return function (target, name: string) {

        if (target instanceof BaseModel) {

            target._validators = target._validators || {};
            target._errorMessages = target._errorMessages || {};
            target._validators[name] = target._validators[name] || [];
            target._errorMessages[name] = target._errorMessages[name] || {};

            target._errorMessages[name][validatorName] = validatorMessage;
            target._validators[name].push(validatorObject);

        }
    }
};