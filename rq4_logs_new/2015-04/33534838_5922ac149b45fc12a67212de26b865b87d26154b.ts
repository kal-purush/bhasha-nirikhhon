/// <reference path="typings/lodash/lodash.d.ts" />
/// <reference path="typings/q/Q.d.ts" />
import Q = require("q");
export declare type errorList = string[];
export declare type errorMessages = {
    [name: string]: errorList;
};
export interface Validator {
    validate<T>(value: T): Q.Promise<ValidationResult<T>>;
    validatePath<T>(oldValue: T, path: string, newValue?: any, context?: any): Q.Promise<ValidationResult<T>>;
}
export interface ValidationResult<T> {
    isValid: boolean;
    value: T;
    errors: errorMessages;
}
export declare type validationFunction = (input: any, context?: any) => any;
export declare type validatorDefinition = validationFunction | Validator | validationFunction[] | Validator[] | validationFunction[][] | Validator[][] | validationFunction[][][] | Validator[][][] | {
    [name: string]: (validatorDefinition);
} | {
    [name: string]: (validatorDefinition);
}[];
export declare type objectValidatorDef = {
    [name: string]: Validator;
};
export declare type arrayValidatorDef = Validator;
export declare function validator(defs: validatorDefinition): Validator;
export declare function required(input: any, isNot?: any): any;
export declare function str(input: any): string;
export declare function integer(input: any): number;
export declare function float(input: any): number;
export declare function getUsingDotArrayNotation(object: any, notation: string): any;
export declare function setUsingDotArrayNotation<T>(object: T, notation: string, val: any): T;
export declare class FuncValidator implements Validator {
    func: validationFunction;
    constructor(func: validationFunction, parent?: any);
    validatePath<T>(oldValue: T, path: string, newValue?: any, context?: any): Q.Promise<ValidationResult<T>>;
    validate<T>(value: T): Q.Promise<ValidationResult<T>>;
}
export declare class ObjectValidator implements Validator {
    fields: objectValidatorDef;
    constructor(fields: objectValidatorDef);
    validatePath<T>(oldValue: T, path: string, newValue?: any, context?: any): Q.Promise<ValidationResult<T>>;
    validate<T>(object: T): Q.Promise<ValidationResult<T>>;
}
export declare class ArrayValidator implements Validator {
    validator: Validator;
    constructor(validator: Validator);
    validatePath<T>(oldValue: T, path: string, newValue?: any, context?: any): Q.Promise<ValidationResult<T>>;
    validate<T>(arr: T[]): Q.Promise<ValidationResult<T>>;
}