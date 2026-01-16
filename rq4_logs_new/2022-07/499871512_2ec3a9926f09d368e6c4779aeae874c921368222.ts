
import {Pattern, Token, TokenizerState, Compiler} from '@bmoor/compiler';

export class AccessorToken {

}

export class ArrayToken {

}

export class DotPattern {
	open(str: string, pos: number) {
		const ch = str[pos];

		if (this.pattern.test(ch)) {
			return new TokenizerState(pos);
		}

		return null;
	}
}

export class BracketPattern {

}

export class Parser extends Compiler {
	constructor(){
		super({
			tokenizer: [
				new DotPattern(),
				new BracketPattern()
			],
			reducer: []
		})
	}
}