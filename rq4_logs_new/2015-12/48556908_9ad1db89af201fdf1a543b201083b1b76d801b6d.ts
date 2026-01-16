declare module Bucks {

    export interface SingleMethods<T extends Element> {
        <T extends Element>(selector: string): SingleResult<T>;
        find<T extends Element>(selector: string): SingleResult<T>;
        visible(): SingleResult<T>;
        withAttribute(attributeName: string, regExp: RegExp): SingleResult<T>;
        getAttribute(attributeName: string): string;
        closest<T extends Element>(selector: string): SingleResult<T>;
        all: ListMethods<T>;
    }

    export interface ListMethods<T extends Element> {
        <T extends Element>(selector: string): ListResult<T>;
        find<T extends Element>(selector: string): ListResult<T>;
        visible(): ListResult<T>;
        withAttribute(attributeName: string, regExp: RegExp): ListResult<T>;
    }

    export interface SingleResult<T extends Element> extends SingleMethods<T> {
        found: T;
        maybe: T;
    }

    export interface ListResult<T extends Element> extends SingleMethods<T> {
        found: T[];
        maybe: T[];
    }

    export interface Static extends SingleMethods<Element> {
        <T extends Element>(element: T): SingleResult<T>;
        <T extends Element>(element: T[]): ListResult<T>;
    }
}

declare var $$: Bucks.Static;