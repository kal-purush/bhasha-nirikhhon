/// <reference path="bower_components/ho-promise/dist/promise.d.ts" />
declare module ho.classloader {
    let mapping: {
        [key: string]: string;
    };
    class ClassLoader {
        private conf;
        private cache;
        constructor(c?: ILoaderConfig);
        config(c: ILoaderConfig): void;
        load(arg: ILoadArguments): promise.Promise<Function[], any>;
        protected load_script(arg: ILoadArguments): PromiseOfClasses;
        protected load_function(arg: ILoadArguments): PromiseOfClasses;
        protected load_eval(arg: ILoadArguments): PromiseOfClasses;
        protected getParentName(url: string): ho.promise.Promise<string, any>;
        protected parseParentFromSource(src: string): string;
        resolveUrl(name: string): string;
        protected exists(name: string): boolean;
    }
}
declare module ho.classloader {
    interface ILoadArguments {
        name: string;
        url?: string;
        parent: boolean;
        expose: boolean;
        super: string;
    }
    class LoadArguments implements ILoadArguments {
        name: string;
        url: string;
        parent: boolean;
        expose: boolean;
        super: string;
        constructor(arg: ILoadArguments, resolveUrl: (name: string) => string);
    }
}
declare module ho.classloader {
    interface ILoaderConfig {
        loadType: LoadType;
        urlTemplate: string;
        useDir: boolean;
        useMin: boolean;
        exists: (name: string) => boolean;
        cache: boolean;
    }
    class LoaderConfig implements ILoaderConfig {
        loadType: LoadType;
        urlTemplate: string;
        useDir: boolean;
        useMin: boolean;
        exists: (name: string) => boolean;
        cache: boolean;
        constructor(c?: ILoaderConfig);
    }
}
declare module ho.classloader {
    enum LoadType {
        SCRIPT = 0,
        FUNCTION = 1,
        EVAL = 2,
    }
}
declare module ho.classloader {
}
declare module ho.classloader {
    type clazz = Function;
    type PromiseOfClasses = ho.promise.Promise<clazz[], any>;
}
declare module ho.classloader.util {
    function expose(name: string, obj: any): void;
}
declare module ho.classloader.util {
    function get(path: string, obj?: any): any;
}
declare module ho.classloader.xhr {
    function get(url: string): ho.promise.Promise<string, string>;
}