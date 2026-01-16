declare module 'ts-injector' {
  export function Inject(className: string): (target: any, propertyKey: string) => void;

  interface Injector {
    register(className: string, instance: Object): void;
    inject(instance: any): void;
    inject(instance: any, context: Context): void;
  }

  interface Context {
    register(className: string, instance: Object): void;
    get(className: string): Object;
  }

  export class Injector implements Injector {}
  export class Context implements Context {}

  export class MetaData {
    static INJECTS: string;
}
  export interface InjectData {
    name:  string;
    className: string;
  }
}