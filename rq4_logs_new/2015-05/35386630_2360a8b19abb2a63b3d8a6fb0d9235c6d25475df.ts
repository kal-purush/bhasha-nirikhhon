import Tokenizer = require('./Tokenizer');

export class Parser {
	
	private tokens : Array<Tokenizer.Token>;
	private lookAhead : Tokenizer.Token;
	
	constructor(){}

	parse(expression: string) : Path {
		var tokenizer = new Tokenizer.Tokenizer();
		this.tokens = tokenizer.tokenize(expression);
				
		this.lookAhead = this.tokens[0];
		
		var path = this.path();
		
		if (this.lookAhead.token != Tokenizer.TokenType.EPSILON)
      		throw new TypeError("Unexpected symbol '" + this.lookAhead.sequence + "' found");
		
		return path;
	}
	
	private path(): Path {
		var property = this.property();
		if (this.lookAhead.token == Tokenizer.TokenType.DOT) {
			this.nextToken();
			var subPath = this.path();
		}
		return new Path(property, subPath);
	}
	
	private property() : Property {
		if (this.lookAhead.token != Tokenizer.TokenType.IDENTIFIER) {
			throw new TypeError("Unexpected token: '" + this.lookAhead.sequence + "'");	
		}
		
		var property = this.lookAhead.sequence;
		this.nextToken();
	
		return this.arrayIndex(property);
	}

	private arrayIndex(property : string) : Property {
		if (this.lookAhead.token == Tokenizer.TokenType.LSQBRACKET) {
			this.nextToken();
			if (this.lookAhead.token != Tokenizer.TokenType.INTEGER) {
				throw new TypeError("Unexpected token: '" + this.lookAhead.sequence + "'. Expected array index.");	
			}
			var index = parseInt(this.lookAhead.sequence);
			this.nextToken();
			if (this.lookAhead.token != Tokenizer.TokenType.RSQBRACKET) {
				throw new TypeError("Unexpected token: '" + this.lookAhead.sequence + "'. Expected ']'");	
			}
			this.nextToken();
			return new ArrayItemProperty(property, index);
		}
		return new Property(property);
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

 
export class Path {
	protected property : Property;
	protected subPath : Path;
	
	constructor(property : Property, subPath : Path) {
		this.property = property;
		this.subPath = subPath;
	}
	toString() {
		return this.property.toString() + (this.subPath != null? "." + this.subPath.toString() : "");
	}
}

export class Property {
	protected property : string;
	
	constructor(property : string) {
		this.property = property;
	}
	toString() {
		return this.property;
	}
}


export class ArrayItemProperty extends Property {
	protected index : number;
	
	constructor(property : string, index : number) {
		super(property);
		this.index = index;
	}
	toString() {
		return super.toString() + "[" + this.index + "]";
	}
}
