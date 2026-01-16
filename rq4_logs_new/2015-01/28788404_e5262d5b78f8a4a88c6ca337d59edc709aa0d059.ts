module Validation {
    export interface StringValidator {
        isAcceptable(s: string): boolean;
    }

    var minLength = 5;
    export class MinLengthValidator implements StringValidator {
        isAcceptable(s: string): boolean {
            return s.length >= minLength;
        }
    }

    var lettersRegExp = /^[a-z]+$/i;
    export class LettersOnlyValidator implements StringValidator {
        isAcceptable(s: string): boolean {
            return lettersRegExp.test(s);
        }
    }
}


var validators: { [x: string]: Validation.StringValidator } = {};
validators['Min Length'] = new Validation.MinLengthValidator();
validators['Letters Only'] = new Validation.LettersOnlyValidator();
['ddddd', 'ss', '23sss'].forEach(s => {
    for(var v in validators) {
        console.log(s + ' ' + v + ' ' + (validators[v].isAcceptable(s)?'ok':'bad'));
    }
});