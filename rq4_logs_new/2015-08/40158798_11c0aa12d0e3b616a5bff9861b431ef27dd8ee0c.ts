declare module "url" {
    export interface Url {
        protocol?: string;
        hostname?: string;
        pathname?: string;
    }

    export function parse(urlStr: string, parseQueryString?, slashesDenoteHost?): Url;
}

declare module "someOtherModule" {
    export function separate(phrase: string, indexToSeperateAt: number): string;
    export function join(separator: string, ...words: string[]): string;
    export var count: number;
}