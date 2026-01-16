import { ComputedRef, Ref } from '@vue/reactivity';
import { Context, contexts } from './Context';
import { managementWrapper, setupWrapper } from './contructors';
import { useStoreDevtools } from './devtools';

export interface StateFactory {
	[key: string]: Ref | ComputedRef | Function;
}

export interface ManagementFactory {
	[key: string]: Function;
}

const config = {
	dev: true
};

export class Widok {
	static config(options: Partial<typeof config>) {
		Object.assign(config, options);
	}

	static defineStore<T extends StateFactory, R extends ManagementFactory>(
		name: string,
		stateFactory: () => T,
		managementFactory: (state: T) => R = () => ({} as R)
	): [() => T & R, Function] {
		return [
			useStore<T & R>(name, stateFactory, managementFactory),
			destroyStore(name)
		];
	}
}

function useStore<T>(name: string, s: Function, m: Function): () => T {
	return () => {
		let context = contexts.get(name);

		if (!context) {
			const context = Context.init(name);
			const state = setupWrapper(s());
			const management = managementWrapper(m(state, teardownManagement));
			const instance = Object.assign({}, state, management);

			contexts.set(name, context);
			config.dev && useStoreDevtools(context);
			Context.setup(instance);

			return instance;
		}

		return context.instance;
	}
}

function destroyStore(name: string) {
	return () => {
		const context = contexts.get(name);

		if (context) {
			context.destroy();
		}
	}
}

function teardownManagement(cb: Function) {
	const context = contexts.get(name);
	context.teardown.push(cb);
}