export class Lexer{
    expression: string;
    index: number =  0;
    constructor(expression: string){
        this.expression = expression + "\0";
    }

    current(): string{
        return this.expression[this.index];
    }

}