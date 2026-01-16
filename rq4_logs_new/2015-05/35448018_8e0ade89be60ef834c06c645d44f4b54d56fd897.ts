import { IMaxConfig, BaseModel, MaxValidator } from'../main';


export let max = function (config: IMaxConfig) {
    return function (target, name: string) {

        if (target instanceof BaseModel) {
            target._validators = target._validators || {};
            target._errorMessages = target._errorMessages || {};
            target._validators[name] = target._validators[name] || [];
            target._errorMessages[name] = target._errorMessages[name] || {};


            target._errorMessages[name].max = config.message;
            target._validators[name].push(new MaxValidator(config));
        }
    }
};