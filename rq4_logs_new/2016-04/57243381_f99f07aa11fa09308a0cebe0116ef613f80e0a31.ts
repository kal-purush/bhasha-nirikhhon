import { DataSource } from 'falcor'
import { PathSet, JSONGraph, PathValue, JSONEnvelope } from 'falcor-json-graph'
import { Thenable } from 'es6-promise'

declare class Router extends DataSource {
  constructor (routes: Array<Router.Definition>, options?: Router.Options);

  /**
   * When a route misses on a call, get, or set the unhandledDataSource will
   * have a chance to fulfill that request.
   */
  routeUnhandledPathsTo (dataSource: DataSource): void;

  static createClass (routes?: Array<Router.Definition>): Router;
}

declare namespace Router {
  export interface Options {
    debug?: boolean;
    maxPaths?: number;
    maxRefFollow?: number;
  }

  export interface Route {
    route: string;
  }

  export interface CallRoute extends Route {
    call (callPath: PathSet, args: Array<any>): Result<any> | Thenable<Result<any>>;
  }

  export interface GetRoute extends Route {
    get (pathset: PathSet): Result<any> | Thenable<Result<any>>;
  }

  export interface SetRoute extends Route {
    set (jsonGraph: JSONGraph): Result<any> | Thenable<Result<any>>;
  }

  export type Definition = GetRoute | SetRoute | CallRoute;

  export type Result <T> = PathValue<T> | Array<PathValue<T>> | JSONEnvelope<T>;

  export var ranges: string;
  export var integers: string;
  export var keys: string;

  export class JSONGraphError extends Error {
    constructor (typeValue: any);
    typeValue: any;
  }
}

export = Router;