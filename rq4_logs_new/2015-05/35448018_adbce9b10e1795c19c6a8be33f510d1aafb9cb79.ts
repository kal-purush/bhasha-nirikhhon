import { BaseModel } from '../main';
import { IMinConfig } from '../main';
import { MinValidator } from '../main';

/**
 * identifies the property it decorates as having a minimum value of config.min.
 * will treat '7' as 7 for validation purposes
 *
 * @param config configuration with min and message properties
 * @returns {function(any, string): undefined}
 */
export let min = function (config: IMinConfig) {

    return function (target, name: string) {

        if (target instanceof BaseModel) {
            target._validators = target._validators || {};
            target._errorMessages = target._errorMessages || {};
            target._validators[name] = target._validators[name] || [];
            target._errorMessages[name] = target._errorMessages[name] || {};

            target._errorMessages[name].min = config.message;

            target._validators[name].push(new MinValidator(config));
        }
    };
};