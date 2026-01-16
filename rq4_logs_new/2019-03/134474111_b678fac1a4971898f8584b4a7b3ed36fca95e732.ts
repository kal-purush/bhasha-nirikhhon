//tslint:disable-next-line:no-var-requires
const svc: any = require('polyfill-library');

export type PolyfillFlag = 'gated' | 'always';
export type Unknown = 'polyfill' | 'ignore';

export interface Feature {
  flags?: PolyfillFlag[];
}

export interface Features {
  [feature: string]: Feature;
}

export interface GetPolyfillsOptions {
  excludes?: string[];

  features?: Features;

  uaString: string;
}

export interface GetPolyfillStringOptions extends GetPolyfillsOptions {
  minify?: boolean;

  unknown?: Unknown;
}

export interface PolyfillSpec {
  aliasOf?: Set<string>;

  flags: Set<PolyfillFlag>;
}

export interface GetPolyfillsResponse {
  [name: string]: PolyfillSpec;
}

export function listAllPolyfills(): Promise<ReadonlyArray<string>> {
  return svc.listAllPolyfills();
}

export function getPolyfillString(options: GetPolyfillStringOptions): Promise<string> {
  return svc.getPolyfillString(options);
}

export function getPolyfills(options: GetPolyfillsOptions): Promise<GetPolyfillsResponse> {
  return svc.getPolyfills(options);
}