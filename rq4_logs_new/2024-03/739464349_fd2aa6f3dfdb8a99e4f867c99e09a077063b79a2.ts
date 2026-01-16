const { DEV } = import.meta.env;

export function isDev(): boolean {
	return DEV;
}