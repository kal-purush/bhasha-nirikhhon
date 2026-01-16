import validation = require('./Validation');

var lettersRegex = /^[A-Za-z]+$/;
export class LettersOnlyValidator implements validation.StringValidator {
	isAcceptable(s:string){
		return lettersRegex.test(s);
	}
}