declare module "plist" {
	export function parse(value: string): any;
	export function build(value: any): string;
}

declare module "json-merge-patch" {
	export function apply(v1: string, v2: string): any;
}