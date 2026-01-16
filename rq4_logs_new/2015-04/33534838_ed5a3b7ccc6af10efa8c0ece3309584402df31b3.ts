/// <reference path="typings/lodash/lodash.d.ts" />
/// <reference path="typings/when/when.d.ts" />

import _ = require("lodash");
import Q = require("q");

export type errorMessages = {
    [name: string] : string[]
}

export type validationFunction =
    ((input: any, cleaned: any, errors: errorMessages) => ValidationResult<any> | any) |
    Validator;

export type validationDefinition =
    validationFunction |
    { [name: string] : validationFunction };

export type fieldValidators = {
    [name: string] : validationDefinition
}

export interface ValidationResult<T> {
    isValid: boolean
    value: T
    errors: errorMessages
}

export function required(input: any, isNot: any = null): any {
    if (!input) {
        throw "This field is required";
    }
    return input;
}

export function str(input: any, def: number = 0): string {
    return "" + input;
}

export function integer(input: any, def: number = 0): number {
    return parseInt("" + input) || def;
}

export function float(input: any, def: number = 0.0): number {
    return parseFloat("" + input) || def;
}

export interface Validator {
    validate<T>(object: T): Q.Promise<ValidationResult<T>>
}

function getValidator(fields: fieldValidators, fieldName: string) {
    var validFunc = fields[fieldName];
    if (_.isFunction(validFunc)) {
        return validFunc;
    } else if (_.isPlainObject(validFunc)) {
        return new ObjectValidator(<{ [name: string] : validationFunction }> validFunc);
    } else if (_.isArray(validFunc)) {
        return new ArrayValidator(validFunc[0]);
    }
    throw "Validator is not defined for this field";
}

export class ObjectValidator implements Validator {
    fields: fieldValidators
    constructor(fields: fieldValidators) {
        this.fields = fields;
    }
    
    validateField<T>(object: T, fieldName: string, newValue: any):
        Q.Promise<ValidationResult<T>>
    {
        var errors: errorMessages = {},
            fieldErrors: string[] = [],
            isValid = false;
            
        errors[fieldName] = fieldErrors;
        try {
            var validFunc: any = getValidator(this.fields, fieldName);
        } catch (e) {
            fieldErrors.push(e);
            return Q.resolve({
                isValid : false,
                value : object,
                errors : errors
            })
        }
        
        var copy = _.cloneDeep(object);
        
        // Probably implements validator
        if (_.isObject(validFunc) && "validate" in validFunc) {
            var defer = Q.defer<ValidationResult<T>>();
            (<Validator> validFunc).validate<T>(newValue).then((res: ValidationResult<T>) => {
                if (res.isValid) {
                    copy[fieldName] = res.value;
                }
                defer.resolve({
                    isValid : res.isValid,
                    value : copy,
                    errors : res.errors // TODO PREFIX ERRORS
                });
            });
            return defer.promise;
            
        // Is probably a plain validator function
        } else {
            var res = object[fieldName];
            try {
                res = (<any>validFunc)(newValue, object, errors);
            } catch (e) {
                errors[fieldName].push("" + e);
            }
        }
        
        // Is ValidationResult
        if (_.isPlainObject(res) &&
            "isValid" in res && "value" in res && "errors" in res)
        {
            copy[fieldName] = res.value;
            fieldErrors.push(res.errors);
            return Q.resolve({
                isValid : res.isValid,
                value : copy,
                errors : errors
            });
            
        // Is probably a cleaned and valid value
        } else if (!errors[fieldName].length) {
            copy[fieldName] = res;
            delete errors[fieldName];
            isValid = true;
        }
        return Q.resolve({
            isValid : isValid,
            value : copy,
            errors : errors
        });
    }
    
    validate<T>(object: T): Q.Promise<ValidationResult<T>> {
        var self = this,
            defer = Q.defer<ValidationResult<T>>(),
            dfields: Q.Promise<ValidationResult<T>>[] = [];
            
        var copy = _.cloneDeep(object),
            errors: errorMessages = {};
        
        _.each(object, function(v, k) {
            var p = self.validateField(object, k, v);
            dfields.push(p);
            p.then((res) => {
                if (res.isValid) {
                    copy[k] = res.value[k];
                }
                if (res.errors[k].length) {
                    _.assign(errors, res.errors);
                }
            });
        });
        
        // Assumes Q.all promise is resolved after all individual promise
        // callbacks has resolved
        Q.all(dfields).then((resz) => {
            var isValid = true;
            _.each(resz, (res) => {
                if (!res.isValid) {
                    isValid = false;
                }
            });
            defer.resolve({
                isValid : isValid,
                value : <T> copy,
                errors : <errorMessages> errors
            });
        });
        
        return defer.promise;
    }
}

export class ArrayValidator implements Validator {
    fields: fieldValidators
    constructor(fields: fieldValidators) {
        this.fields = fields;
    }
    
    validate<T>(object: T): Q.Promise<ValidationResult<T>> {
        return null;
    }
}