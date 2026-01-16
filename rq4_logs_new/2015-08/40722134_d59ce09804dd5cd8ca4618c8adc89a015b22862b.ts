class CircularBuffer<T>{
	constructor(public size: number){}
	buffer: T[] = [];
	push(el: T): number {
		this.buffer.push(el);
		if (this.buffer.length > this.size) {
			this.buffer.shift();
		}
		return this.buffer.length;
	}
	pop(): T {
		return this.buffer.pop();
	}
}