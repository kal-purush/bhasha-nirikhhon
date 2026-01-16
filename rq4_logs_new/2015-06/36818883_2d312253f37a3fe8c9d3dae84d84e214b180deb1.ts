class Lazy<T> {
	private hasValue: boolean = false;
	private value: T;
	constructor(private getter: () => T) {
	}
	
	public getValue(): T {
		if (!this.hasValue) {
			this.value = this.getter();
			this.hasValue = true;
		}
		return this.value;
	}
}

export = Lazy;