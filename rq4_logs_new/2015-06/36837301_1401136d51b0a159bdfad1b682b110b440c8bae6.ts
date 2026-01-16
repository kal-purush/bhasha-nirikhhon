declare module "hound" {
	export interface Watcher {
		on: (event: string, cb:(file:string, stats:any)=>void)=> void
	}

	export function watch(root: string): Watcher;
	export function unwatch(root: string): void;
	export function clear(): void;
}