import { Promise } from 'es6-promise'

declare function compose <T> (middleware: Array<compose.Middleware<T>>): (context: T, next?: () => any) => Promise<any>;

declare namespace compose {
  export type Middleware <T> = (context: T, next: () => any) => any
}

export = compose;