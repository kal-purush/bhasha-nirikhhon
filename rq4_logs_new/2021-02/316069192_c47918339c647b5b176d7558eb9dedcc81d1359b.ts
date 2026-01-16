import { Injectable } from "@angular/core";
import { BracketBehavior } from "./bracket.behavior";
import { IBehavior } from "./interface.behavior";
import { NumericBehavior } from "./numeric.behavior";
import { OperationBehavior } from "./operation.behavior";

@Injectable()
export class BehaviorProvider {
    constructor(private bracketBehavior: BracketBehavior, private numericBehavior: NumericBehavior, private operationBehavior: OperationBehavior) {

    }

    /**
     * GetBehavior
     */
    public GetBehavior(key: string): IBehavior {
        switch (key) {
            case '(':
            case ')':

                return this.bracketBehavior;
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '0':
            case '.':

                return this.numericBehavior;
            case '+':
            case '-':
            case '*':
            case '/':
            case '^':

                return this.operationBehavior;
            default:
                return this.operationBehavior;
        }
    }
}