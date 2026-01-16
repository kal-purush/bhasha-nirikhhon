/// <reference path="../node_modules/lz-tslib-interfaces/ISpan" />
/// <reference path="../node_modules/lz-tslib-interfaces/FGetter" />

class EndDerrivedSpan implements ISpan {
	constructor(
		private fGetStartIndex: FGetter<number>,
		private fGetLength: FGetter<number>) {
	}

	getStartIndex(): number {
		return this.fGetStartIndex();
	}

	getLength(): number {
		return this.fGetLength();
	}

    getEndIndex(): number {
		return this.fGetStartIndex() + this.fGetLength() - 1;
	}
}

export = EndDerrivedSpan;