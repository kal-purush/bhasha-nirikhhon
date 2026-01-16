import {Token, Constant, Operator, Associativity, Parenthesis, ParenDirection } from './token';

export class Arithmetic {
    static toPolishNotation(inFix: Token[]): Token[] {
        let stack: Token[] = [];
        let queue: Token[] = [];

        while(inFix.length > 0) {
            let token: Token = inFix.shift();
            if(token instanceof Constant) {
                queue.push(token);
            }
            else if(token instanceof Operator) {
                // While the top of the stack is an Operator,
                // and either:
                // token is left-associative and its precedence is <= to the top's.
                // or token is right-associative and its precendence is < to the top's.
                while(stack[0] instanceof Operator &&
                    (
                        (
                            (<Operator>token).associativity === Associativity.Left
                            && (<Operator>token).precedence <= (<Operator>stack[0]).precedence
                        )
                        || (
                            (<Operator>token).associativity === Associativity.Right
                            && (<Operator>token).precedence < (<Operator>stack[0]).precedence)
                        )
                    ) {
                    queue.push(stack.pop());
                }
                stack.push(token);
            }
            else if(token instanceof Parenthesis) {
                if((<Parenthesis>token).direction === ParenDirection.Left) {
                    stack.push(token);
                }
                else if((<Parenthesis>token).direction === ParenDirection.Right) {
                    while(stack.length > 0 && !(stack[0] instanceof Parenthesis) || (<Parenthesis>stack[0]).direction !== ParenDirection.Left) {
                        queue.push(stack.pop());
                    }
                    if(stack.length === 0) {
                        throw Error("Mismatch parentheses.");
                    }
                    stack.pop();
                }
            }
        }

        while(stack.length > 0) {
            if(stack[0] instanceof Parenthesis) {
                throw Error("Mismatch parentheses.");
            }
            queue.push(stack.pop());
        }

        return queue;
    }
    static eval(expression: Token[]): number {
        let stack: Token[] = [];
        while(expression.length > 0) {
            let token: Token = expression.shift();
            if(token instanceof Constant) {
                stack.push(token);
            }
            else if(token instanceof Operator) {
                let values: number[] = [];
                for(let j = 0; j < token.numOfArgs; j++) {
                    values.unshift((<Constant>stack.pop()).value);
                }
                let result = (<Operator>token).action(values);
                stack.push(new Constant(result.toString(), result));
            }
        }

        console.log(stack);
        return (<Constant>stack.pop()).value;
    }
}