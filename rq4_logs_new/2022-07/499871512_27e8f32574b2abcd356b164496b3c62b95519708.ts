import {CompilerInterface} from '../compiler.interface';
import {Pattern} from '../tokenizer/pattern';
import {Token} from '../tokenizer/token';
import {TokenizerState} from '../tokenizer/state';
import {Expressable, Usages} from '../expressor/expressable';

export class BlockToken extends Token {
	static reference = 'block';

	toExpressable(compiler: CompilerInterface) {
		const fn = compiler.compile(this.content);

		return [new Expressable(Usages.value, fn)];
	}
}

export class BlockPattern extends Pattern {
	begin: string;
	end: string;
	count: number;

	constructor(begin: string, end: string) {
		super();

		this.begin = begin;
		this.end = end;
		this.count = 1;
	}

	open(str: string, pos: number) {
		const compare = str.substring(pos - this.begin.length + 1, pos + 1);

		if (compare === this.begin) {
			return new TokenizerState(pos + 1);
		}

		return null;
	}

	close(str: string, pos: number) {
		if (str.substring(pos, pos + this.begin.length) === this.begin) {
			this.count++;
		} else if (str.substring(pos, pos + this.end.length) === this.end) {
			this.count--;

			if (this.count === 0) {
				return pos;
			}
		}

		return null;
	}

	toToken(base: string, state: TokenizerState) {
		return new BlockToken(base.substring(state.begin, state.end + 1), state);
	}
}