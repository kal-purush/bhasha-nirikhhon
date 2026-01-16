/**
 * Created by cash on 2015/05/12.
 */

//---- JS 的程式

//interface StringValidator{
//    isAcceptable(s:string):boolean;
//}
//
//var letterRegexp = /^[A-Za-z]+$/;
//var numberRegexp = /^[0-9]+$/;
//
//class LettersOnlyValidator implements StringValidator{
//
//    isAcceptable(s:string):boolean {
//        return letterRegexp.test(s);
//    }
//}
//
//class ZipCodeValidator implements StringValidator{
//
//    isAcceptable(s:string):boolean {
//        return s.length === 5 && numberRegexp.test(s);
//    }
//}

//---- typescript 寫法

module Validation {
    export interface StringValidator{
        isAcceptable(s:string):boolean;
    }

    var letterRegexp = /^[A-Za-z]+$/;
    var numberRegexp = /^[0-9]+$/;

    export class LettersOnlyValidator implements StringValidator{

        isAcceptable(s:string):boolean {
            return letterRegexp.test(s);
        }
    }

    export class ZipCodeValidator implements StringValidator{

        isAcceptable(s:string):boolean {
            return s.length === 5 && numberRegexp.test(s);
        }
    }
}
