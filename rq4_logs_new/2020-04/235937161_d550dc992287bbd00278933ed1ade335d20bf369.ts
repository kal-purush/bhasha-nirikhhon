
import { computed, isValue, EnValue } from './value';
export interface AsValue<T> {
	<K extends keyof T>(key: K): EnValue<T[K]>
}

function createValue<T, K extends keyof T>(
	props: T,
	key: K,
): EnValue<T[K]> {
	return computed(() => {
		const p = props[key];
		return isValue(p) ? p() : p;
	}, v => {
		const p = props[key];
		if (isValue(p)) {
			p(v);
		} else {
			props[key] = v;
		}
	});
}
export function asValue<T>(props: T): AsValue<T>;
export function asValue<T, K extends keyof T>(
	props: T,
	key: K,
): EnValue<T[K]>;
export function asValue<T, K extends keyof T>(
	props: T,
	key?: K,
): AsValue<T> | EnValue<T[K]> {
	if (arguments.length >= 2) {
		return createValue(props, key!);
	}
	return k => createValue(props, k);
}