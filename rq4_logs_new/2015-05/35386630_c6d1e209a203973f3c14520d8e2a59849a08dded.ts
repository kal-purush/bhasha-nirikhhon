import Tokenizer = require('./Tokenizer');

export class Parser {
	
	private tokens : Array<Tokenizer.Token>;
	private lookAhead : Tokenizer.Token;
	
	constructor(){}

	parse(expression: string) : Expression {
		var tokenizer = new Tokenizer.Tokenizer();
		this.tokens = tokenizer.tokenize(expression);
				
		this.lookAhead = this.tokens[0];
		
		var expr = this.expression();
		
		if (this.lookAhead.token != Tokenizer.TokenType.EPSILON)
      		throw new TypeError("Unexpected symbol '" + this.lookAhead.sequence + "' found");
		
		return expr;
	}
	
	private expression(): Expression {
		var expr = this.orExpression();
		return expr;
	}
	
	private orExpression() : Expression {
		var expr = this.andExpression();
		if (this.lookAhead.token == Tokenizer.TokenType.OR) {
			this.nextToken();
			var expr2 = this.orExpression();
			return new OrExpression(expr, expr2);
		}
		return expr;
	}

	private andExpression() : Expression {
		var expr = this.boolExpression();
		if (this.lookAhead.token == Tokenizer.TokenType.AND) {
			this.nextToken();
			var expr2 = this.andExpression();
			return new AndExpression(expr, expr2);
		}
		return expr;
	}	
	
	private boolExpression() : Expression {
		var expr = this.addExpression();
		if (this.lookAhead.token == Tokenizer.TokenType.EQUAL) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new EqualExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.NOTEQUAL) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new NotEqualExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.LESS) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new LessExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.LESSEQ) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new LessEqExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.GREATER) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new GreaterExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.GREATEREQ) {
			this.nextToken();
			var expr2 = this.boolExpression();
			return new GreaterEqExpression(expr, expr2);
		}
		
		return expr;
	}	
	
	private addExpression() : Expression {
		var expr = this.multExpression();
		if (this.lookAhead.token == Tokenizer.TokenType.PLUS) {
			this.nextToken();
			var expr2 = this.addExpression();
			return new AdditionExpression(expr, expr2);
		}
		else if (this.lookAhead.token == Tokenizer.TokenType.MINUS) {
			this.nextToken();
			var expr2 = this.addExpression();
			return new SubtractionExpression(expr, expr2);
		}
		return expr;
	}
	
	private multExpression() : Expression {
		var expr = this.atom();
		if (this.lookAhead.token == Tokenizer.TokenType.MULTIPLICATION) {
			this.nextToken();
			var expr2 = this.multExpression();
			return new MultExpression(expr, expr2);
		} else if (this.lookAhead.token == Tokenizer.TokenType.DIVISION) {
			this.nextToken();
			var expr2 = this.multExpression();
			return new DivisionExpression(expr, expr2);
		}
		return expr;
	}
	
	private atom() : Expression {
		if (this.lookAhead.token == Tokenizer.TokenType.LPAREN) {
			this.nextToken();
			var expr = this.expression();
			if (this.lookAhead.token != Tokenizer.TokenType.RPAREN) {
				throw new TypeError("Unexpected token: '" + this.lookAhead.sequence + "'");	
			}
			this.nextToken();
			return new ParenthesisExpression(expr);
		} else if (this.lookAhead.token == Tokenizer.TokenType.NOT) {
			this.nextToken();
			var expr = this.expression();
			return new NotExpression(expr);
		} 
		else if (this.lookAhead.token == Tokenizer.TokenType.IDENTIFIER) {
			return this.identifier();
		} else if (this.lookAhead.token == Tokenizer.TokenType.STRING) {
			var str = this.lookAhead.sequence.substring(1, this.lookAhead.sequence.length - 1);
			this.nextToken();
			return new StringConstant(str);
		} else if (this.lookAhead.token == Tokenizer.TokenType.INTEGER) {
			var number = parseInt(this.lookAhead.sequence, 10);
			this.nextToken();
			return new IntegerConstant(number);
		} else if (this.lookAhead.token == Tokenizer.TokenType.DECIMAL) {
			var number = parseFloat(this.lookAhead.sequence);
			this.nextToken();
			return new DecimalConstant(number);
		} else if (this.lookAhead.token == Tokenizer.TokenType.TRUE || this.lookAhead.token == Tokenizer.TokenType.FALSE) {
			var bool = this.lookAhead.sequence ? (this.lookAhead.sequence.toLowerCase() == "true") : false
			this.nextToken();
			return new BooleanConstant(bool);
		} else {
			throw new TypeError("Unexpected token: '" + this.lookAhead.sequence + "'");	
		}
	}
	
	private identifier() : Expression {
		var identifier = this.lookAhead.sequence;
		this.nextToken();
		if (this.lookAhead.token == Tokenizer.TokenType.LPAREN) {
			this.nextToken();
			var expr = new FunctionCallExpression(identifier);
			return this.functionCall(expr);
		}
		return new IdentifierExpression(identifier);
	}
	
	
	private functionCall(functionCall : FunctionCallExpression) : Expression {
		if (this.lookAhead.token != Tokenizer.TokenType.RPAREN) {
			this.functionCallArgument(functionCall);
		} 
		if (this.lookAhead.token == Tokenizer.TokenType.RPAREN) {
			this.nextToken();
			return functionCall;
		} else {
			throw new TypeError("Unexpected argument in function call");
		}
	}
	
	private functionCallArgument(functionCall : FunctionCallExpression) : Expression {
		var arg = this.expression();
		functionCall.addArgument(arg);
		if (this.lookAhead.token == Tokenizer.TokenType.COMMA) {
			this.nextToken();
			return this.functionCallArgument(functionCall);
		} 
		return functionCall;
	}
	
	
  /**
   * Remove the first token from the list and store the next token in lookahead
   */
  private nextToken() : void
  {
    this.tokens.shift();
    // at the end of input we return an epsilon token
    if (this.tokens.length === 0)
      this.lookAhead = new Tokenizer.Token(Tokenizer.TokenType.EPSILON, "", -1);
    else
      this.lookAhead = this.tokens[0];
  }
}





export class Expression {
	getValue() : any {
		return 0;
	}
}
export class UnaryExpression extends Expression {
	protected expr : Expression;
	
	constructor(expr : Expression) {
		super();
		this.expr = expr;
	} 
	getValue() : any {
		return this.expr.getValue();
	}
}
export class BinaryExpression extends Expression {
	
	protected expr1 : Expression;
	protected expr2 : Expression;
	private operator : string;
	
	constructor(operator : string, expr1 : Expression, expr2 : Expression) {
		super();
		this.operator = operator;
		this.expr1 = expr1;
		this.expr2 = expr2;
	} 
	toString() : string {
		return this.expr1.toString() + " " + this.operator + " " + this.expr2.toString();
	}
}

export class OrExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("||", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() || this.expr2.getValue();
	}
}

export class AndExpression extends BinaryExpression {	
	constructor(expr1 : Expression, expr2 : Expression) {
		super("&&", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() && this.expr2.getValue();
	}
}

export class EqualExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("==", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() === this.expr2.getValue();
	}
}
export class NotEqualExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("!=", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() !== this.expr2.getValue();
	}
}
export class LessExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("<", expr1, expr2);
	}
	getValue() : boolean {
		return this.expr1.getValue() < this.expr2.getValue();
	}
}
export class LessEqExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("<=", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() <= this.expr2.getValue();
	}
}
export class GreaterExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super(">", expr1, expr2);
	}
	getValue() : boolean {
		return this.expr1.getValue() > this.expr2.getValue();
	} 
}
export class GreaterEqExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super(">=", expr1, expr2);
	} 
	getValue() : boolean {
		return this.expr1.getValue() >= this.expr2.getValue();
	} 
}

export class AdditionExpression extends BinaryExpression {	
	constructor(expr1 : Expression, expr2 : Expression) {
		super("+", expr1, expr2);
	} 	
	
	getValue() : number {
		return this.expr1.getValue() + this.expr2.getValue();
	}
}


export class SubtractionExpression extends BinaryExpression {	
	constructor(expr1 : Expression, expr2 : Expression) {
		super("-", expr1, expr2);
	} 	
	
	getValue() : number {
		return this.expr1.getValue() - this.expr2.getValue();
	}
}

export class MultExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("*", expr1, expr2);
	}
	
	getValue() : number {
		return this.expr1.getValue() * this.expr2.getValue();
	}
}
export class DivisionExpression extends BinaryExpression {
	constructor(expr1 : Expression, expr2 : Expression) {
		super("/", expr1, expr2);
	}
	
	getValue() : number {
		return this.expr1.getValue() / this.expr2.getValue();
	}
}

export class StringConstant extends Expression {
	private str : string;
	
	constructor(str : string) {
		super();
		this.str = str;
	} 
	getValue(): string {
		return this.str;
	}
	toString() : string {
		return "\"" + this.str + "\"";
	}
}

export class IntegerConstant extends Expression {
	
	private number : number;
	
	constructor(number : number) {
		super();
		this.number = number;
	} 
	getValue() : number {
		return this.number;
	}
	toString() : string {
		return this.number.toString();
	}
}

export class DecimalConstant extends Expression {
	
	private number : Number;
	
	constructor(number : Number) {
		super();
		this.number = number;
	} 
	getValue(): Number {
		return this.number;
	}
	toString() : string {
		return this.number.toString();
	}
}
export class BooleanConstant extends Expression {
	
	private bool : boolean;
	
	constructor(bool : boolean) {
		super();
		this.bool = bool;
	} 
	getValue(): boolean {
		return this.bool;
	}
	toString() : string {
		return this.bool.toString();
	}
}

export class IdentifierExpression extends Expression {
	
	private identifier : string;
	
	constructor(identifier : string) {
		super();
		this.identifier = identifier;
	} 
	toString() : string {
		return this.identifier;
	}
}

export class FunctionCallExpression extends Expression {
	
	private functionIdentifier : string;
	private arguments : Array<Expression> = [];
	
	constructor(functionIdentifier : string) {
		super();
		this.functionIdentifier = functionIdentifier;
	} 
	
	addArgument(expr : Expression) : void {
		this.arguments.push(expr);
	}
	toString() : string {
		return this.functionIdentifier + "(" + this.arguments.join(", ") + ")";
	}
}

export class ParenthesisExpression extends UnaryExpression {
	constructor(expr : Expression) {
		super(expr);
	} 
	toString() : string {
		return "(" + this.expr + ")";
	}
}

export class NotExpression extends UnaryExpression {
	constructor(expr : Expression) {
		super(expr);
	} 
	getValue(): boolean {
		return !this.expr.getValue();
	}
	toString() : string {
		return "!" + this.expr;
	}
}