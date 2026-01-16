/// <reference path="node_modules/lz-tslib-interfaces/IEventSubscription" />
/// <reference path="node_modules/lz-tslib-interfaces/IEventEmitter" />

class EventEmitter implements IEventEmitter {
	private subscriptions: IEventSubscription[];

	constructor() {
		this.subscriptions = [];
	}

	on(event: string, callback: Function) {
		this.subscriptions.push({
			subscriber: arguments.callee.caller,
			event: event,
			callback: callback
		});
	}
	
	protected emit(event: string, args: IArguments) {
		for (var index = 0; index < this.subscriptions.length; index++) {
			var subscription = this.subscriptions[index];
			
			if(subscription.event == event) {
				subscription.callback.apply(this, args);
			}
		}
	}
}

export = EventEmitter;