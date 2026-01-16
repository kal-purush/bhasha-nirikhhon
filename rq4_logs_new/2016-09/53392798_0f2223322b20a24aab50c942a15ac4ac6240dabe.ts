import {Request} from "../Http/Request";
import { Inject } from "huject";
var validator = require("validator");

export class Validator {

    @Inject("Request")
    private request: Request;

    private rulesArray: any = {};
    private errorsArray: string[] = [];

    public rules(rules: any) {
        this.rulesArray = rules;
    }

    public validates(): boolean {
        var passed = true;

        for(var field in this.rulesArray) {
            if (this.rulesArray.hasOwnProperty(field)) {
                passed = passed && this.validatesField(field, this.rulesArray[field]);
            }
        }

        return passed;
    }

    public errors() {
        return this.errorsArray;
    }

    public passed() {
        return this.errorsArray.length = 0;
    }

    private validatesField(field:string, rulesString: string): boolean {
        var rules = rulesString.split("|");
        var value = this.request.post(field);

        for (var rule in rules) {
            //TODO parse args and pass to function
            var ruleName = rule;
            var isRuleName = "is" + rule.charAt(0).toUpperCase() + rule.slice(1);

            var result = null;
            if (ruleName in this) {
                result = this[rule]();
            }
            else if(isRuleName in validator) {
                result = validator[isRuleName]()
            } else {
                //TODO add error validation rule not found
                return false;
            }

            if (result) {
                return true;
            } else {
                //TODO get error message
                //TODO addError
                //TODO return false
            }

            console.log(result);
        }

        return false;
    }

    private addError(field: string, message: string) {
        this.errorsArray[field] = message;
    }
}