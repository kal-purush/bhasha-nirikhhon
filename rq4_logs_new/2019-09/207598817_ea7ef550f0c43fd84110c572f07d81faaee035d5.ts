type Fn = (...q: any) => any;
type ListenerSet = Set<Fn>;
type Listeners = Map<string, ListenerSet>;

export default abstract class Houk {
	protected listeners: Listeners = new Map();

	/**
	 * Add an event listener
	 * @param event The event to listen to
	 * @param fn The event handler
	 */
	public on(event: string, fn: Fn): void {
		const listeners = this.getListeners(event);
		listeners.add(fn);
		this.listeners.set(event, listeners);
	}

	/**
	 * Remove an event listener
	 * @param event The event name
	 * @param fn The event handler
	 */
	public off(event: string, fn: Fn): boolean {
		const listeners = this.getListeners(event);
		return listeners.delete(fn);
	}

	/**
	 Emit an event to all listeners.
	 * @param event The event name
	 * @param thisArg The value of `this` which will be passed to the listener function.
	 * @param args Arguments that will be passed to the listener function.
	 */
	protected async emit(
		event: string,
		thisArg: any,
		...args: any
	): Promise<any> {
		const listeners = this.getListeners(event);
		console.log(listeners);

		if (listeners.size === 0) {
			return args.length === 1 ? args[0] : args;
		}

		let result: any = args;
		for (const listener of listeners.values()) {
			const array = toArray(result);
			result =
				(await Promise.resolve(listener.apply(thisArg, array))) || result;
		}

		return result;
	}

	/**
	 * Get all listeners of a particular event
	 * @param event Event name
	 */
	protected getListeners(event: string): ListenerSet {
		return this.listeners.get(event) || new Set();
	}
}

function toArray(input: any): any[] {
	return Array.isArray(input) ? input : [input];
}

if (typeof module !== 'undefined') {
	module.exports = Houk;
	module.exports.default = Houk;
	module.exports.Houk = Houk;
}