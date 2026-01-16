import {Pattern} from '../tokenizer/pattern';
import {Token} from '../tokenizer/token';
import {TokenizerState} from '../tokenizer/state';
import {
	Expressable,
	Usages,
	ExpressableFunction
} from '../expressor/expressable';

export class ValueToken extends Token {
	static reference = 'value';

	toExpressable() {
		return [new Expressable(Usages.value, () => this.content)];
	}
}

export class RegexValuePattern extends Pattern {
	pattern: RegExp;
	subType: string;
	parser: ExpressableFunction;

	constructor(pattern: RegExp, parser: ExpressableFunction, subType: string) {
		super();

		this.pattern = pattern;
		this.parser = parser;
		this.subType = subType;
	}

	open(str: string, pos: number) {
		const ch = str[pos];

		if (this.pattern.test(ch)) {
			return new TokenizerState(pos);
		}

		return null;
	}

	close(str: string, pos: number) {
		const ch = str[pos];

		if (!this.pattern.test(ch)) {
			return pos - 1;
		}

		return null;
	}

	toToken(base: string, state: TokenizerState) {
		return new ValueToken(
			this.parser(base.substring(state.begin, state.end + 1)),
			state,
			{
				subType: this.subType
			}
		);
	}

	getReference() {
		return this.pattern.toString();
	}
}

export class OpToken extends Token {
	static reference = 'operation';

	toExpressable() {
		return [new Expressable(Usages.operation, (a, b) => this.content(a, b))];
	}
}

export class RegexOpPattern extends RegexValuePattern {
	toToken(base: string, state: TokenizerState) {
		return new OpToken(this.parser, state, {
			subType: this.subType
		});
	}
}