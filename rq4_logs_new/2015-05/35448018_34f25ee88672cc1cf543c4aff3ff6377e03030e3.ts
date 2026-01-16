import { BaseModel } from '../main';
import { IValidatorObject } from '../main';
import { NotEmptyValidator } from '../main';

function notEmpty (errorMessage: String) {
    return function (target, name: String, descriptor) {

        if (target instanceof BaseModel) {

            target._validators[name] = target._validators[name] || [];
            target._errorMessages[name] = target._errorMessages[name] || {};


            target._errorMessages[name].notNull = errorMessage;

            target._validators[name].push(new NotEmptyValidator());

        }
    }
}

export { notEmpty };