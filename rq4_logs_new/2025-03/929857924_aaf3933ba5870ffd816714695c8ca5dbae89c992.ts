// eslint-disable-next-line @typescript-eslint/no-explicit-any
const debounce = <T extends (...args: any[]) => ReturnType<T>>(
	callbackFn: T,
	delay: number
): ((...args: Parameters<T>) => ReturnType<T>) => {
	let timer: ReturnType<typeof setTimeout>;

	return (...args: Parameters<T>): ReturnType<T> => {
		clearTimeout(timer);
		return new Promise<ReturnType<T>>((resolve) => {
			timer = setTimeout(() => resolve(callbackFn(...args)), delay);
		}) as ReturnType<T>;
	};
};

export default {
	debounce
};