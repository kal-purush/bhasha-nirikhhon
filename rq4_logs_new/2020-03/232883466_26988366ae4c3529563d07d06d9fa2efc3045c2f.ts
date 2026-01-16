import { InjectionKey } from '@vue/runtime-core';

type EventBusListener<T> = (payload: T) => void;

export class EventBus {
	private eventChannels = new Map<InjectionKey<any>, EventBusListener<any>[]>();

	on<T>(event: InjectionKey<T>, listener: EventBusListener<T>) {
		if (!this.eventChannels.has(event)) {
			this.eventChannels.set(event, []);
		}
		const channel = this.eventChannels.get(event);

		channel.push(listener);

		return () => {
			this.off(event, listener);
		};
	}

	once<T>(event: InjectionKey<T>, listener: EventBusListener<T>) {
		return this.on(event, (payload) => {
			listener(payload);
			this.off(event, listener);
		});
	}

	off(event?: InjectionKey<any>, listener?: EventBusListener<any>) {
		if (event === undefined && listener === undefined) {
			this.eventChannels.clear();
		} else if (listener === undefined) {
			this.eventChannels.delete(event);
		} else {
			const channel = this.eventChannels.get(event) || [];
			const index = channel.indexOf(listener);
			index !== -1 && channel.splice(index, 1);
		}
	}

	emit<T>(event: InjectionKey<T>, payload: T) {
		if (!this.eventChannels.has(event)) {
			return;
		}

		for (const listener of this.eventChannels.get(event) || []) {
			listener(payload);
		}
	}
}