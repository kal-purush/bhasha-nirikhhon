export class Tape{
	constructor(
		private _elements: string[] = [].concat("-"),
		private _position: number = 0
	){}

	public get elements(): string[] {
		return this._elements;
	}

	public set elements(__value: string[]) {
		this._elements = __value;
	}

	public get position(): number {
		return this._position;
	}

	public set position(__value: number) {
		this._position = __value;
	}

	public get actualElement(): string {
		return this._elements[this._position];
	}

	public set actualElement(__value: string) {
		this._elements[this._position] = __value;
	}

	public fixTape(){
		if (this._position === -1){
			this._elements.unshift("-");
			this._position += 1;
		}else if (this._position === this._elements.length) {
			this._elements.push("-");
		}
	}

	public getAlphabet(): string[] {
		return [].concat(...this._elements).filter((__element, __index, __self) => __self.indexOf(__element) === __index);
	}
}