/// <reference path="../../../typings/tsd.d.ts" />

'use strict';

import * as mongoose from 'mongoose';
import * as _ from 'underscore';

/*
 * Initialize a new group of validation functions
 * for a schema property.
 *
 * @param validatorsArray
 */
export function ValidationGroup(validatorsArray) {
    var validationErrors;
  
    /*
     * Iterates through the supplied validators,
     * appending error messages to validationErrors
     * if the validator returns false.
     *
     * @param value
     */
    function validator(value) {
        validationErrors = [];
        var isValid = true;

        validatorsArray.forEach((validator) => {
            if (!validator.validator(value)) {
                // Append message
                validationErrors.push(validator.message);

                isValid = false;
            }
        });
        return isValid;
    }
  
    /* 
     * Simple object to retrieve the list of
     * validation errors.
     *
     * Mongoose expects a string message,
     * but to return a dynamic error message,
     * we need to pass a function that retrieves it.
     */
    function ValidationErrorRetriever() { }

    /*
     * Injects the validation errors when Mongoose
     * first calls Replace on the error message.
     * Retains Mongoose functionality.
     *
     * @param regexp
     * @param newSubStr
     */
    ValidationErrorRetriever.prototype.replace = (regexp, newSubStr) => {
        for (var i = 0; i < validationErrors.length; i++) {
            validationErrors[i] = validationErrors[i].replace(regexp, newSubStr);
        }

        return JSON.stringify(validationErrors);
    };

    /* 
     * For a single validation method, Mongoose expects
     * an array containing the validation method and
     * error message respectively.
     */
    return {
        validator: validator,
        message: new ValidationErrorRetriever()
    };
}

export function phoneNumberValidator(): any {
    var ret = {
        validator: (v: string) => {
            return !!v && v.match('^[+]{0,1}[0-9][0-9\\s-]{8,}$') && !v.match('--') && !v.match('\\s\\s');
        },
        message: '{VALUE} is not a valid phone number!'
    };
    return ret;
}

export function emailValidator(): any {
    var ret = {
        validator: (v: string) => {
            return !!v && v.toUpperCase().match('^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$');
        },
        message: '{VALUE} is not a valid email address!'
    };
    return ret;
}

export function passwordValidator(minLength: number): any {

    var ret = [
        {
            validator: (v: string) => {
                console.log(v);
                return !!v && !!v.match('^.{' + minLength + ',}$');
            },
            message: 'Password length has to be at least ' + minLength + ' characters'
        },
        {
            validator: (v: string) => {
                return !!v && !!v.match('.*[A-Z]');
            },
            message: 'Password must contain uppercase letters'
        },
        {
            validator: (v: string) => {
                return !!v && !!v.match('.*[a-z]');
            },
            message: 'Password must contain lowercase letters'
        },
        {
            validator: (v: string) => {
                return !!v && !!v.match('.*[0-9]')
            },
            message: 'Password must contain digits'
        },
        {
            validator: (v: string) => {
                return !!v && !!v.match('.*[!@#$%^&*_]');
            },
            message: 'Password must contain special characters (!@#$%^&*_)'
        },
        {
            validator: (v: string) => {
                return !!v && !!v.match('^\\S+$');
            },
            message: 'Password must not contain whitespaces'
        }
    ];

    return ret;
}