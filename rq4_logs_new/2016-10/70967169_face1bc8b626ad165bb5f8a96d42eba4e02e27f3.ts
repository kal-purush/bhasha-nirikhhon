export default class EventEmitter {
	protected _fireEvent(event: string, ...args: any[]): void {}

	on(event: string, callback: Function) {}

	off(event: string, callback: Function) {}

	constructor() {}
}