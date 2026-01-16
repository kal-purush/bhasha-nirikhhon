
/**
 * Client
**/

import * as runtime from './runtime/library';
import $Types = runtime.Types // general types
import $Public = runtime.Types.Public
import $Utils = runtime.Types.Utils
import $Extensions = runtime.Types.Extensions
import $Result = runtime.Types.Result

export type PrismaPromise<T> = $Public.PrismaPromise<T>


/**
 * Model assignees
 * 
 */
export type assignees = $Result.DefaultSelection<Prisma.$assigneesPayload>
/**
 * Model tasks
 * 
 */
export type tasks = $Result.DefaultSelection<Prisma.$tasksPayload>
/**
 * Model users
 * 
 */
export type users = $Result.DefaultSelection<Prisma.$usersPayload>

/**
 * ##  Prisma Client ʲˢ
 * 
 * Type-safe database client for TypeScript & Node.js
 * @example
 * ```
 * const prisma = new PrismaClient()
 * // Fetch zero or more Assignees
 * const assignees = await prisma.assignees.findMany()
 * ```
 *
 * 
 * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client).
 */
export class PrismaClient<
  T extends Prisma.PrismaClientOptions = Prisma.PrismaClientOptions,
  U = 'log' extends keyof T ? T['log'] extends Array<Prisma.LogLevel | Prisma.LogDefinition> ? Prisma.GetEvents<T['log']> : never : never,
  ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs
> {
  [K: symbol]: { types: Prisma.TypeMap<ExtArgs>['other'] }

    /**
   * ##  Prisma Client ʲˢ
   * 
   * Type-safe database client for TypeScript & Node.js
   * @example
   * ```
   * const prisma = new PrismaClient()
   * // Fetch zero or more Assignees
   * const assignees = await prisma.assignees.findMany()
   * ```
   *
   * 
   * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client).
   */

  constructor(optionsArg ?: Prisma.Subset<T, Prisma.PrismaClientOptions>);
  $on<V extends U>(eventType: V, callback: (event: V extends 'query' ? Prisma.QueryEvent : Prisma.LogEvent) => void): void;

  /**
   * Connect with the database
   */
  $connect(): $Utils.JsPromise<void>;

  /**
   * Disconnect from the database
   */
  $disconnect(): $Utils.JsPromise<void>;

  /**
   * Add a middleware
   * @deprecated since 4.16.0. For new code, prefer client extensions instead.
   * @see https://pris.ly/d/extensions
   */
  $use(cb: Prisma.Middleware): void

/**
   * Executes a prepared raw query and returns the number of affected rows.
   * @example
   * ```
   * const result = await prisma.$executeRaw`UPDATE User SET cool = ${true} WHERE email = ${'user@email.com'};`
   * ```
   * 
   * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client/raw-database-access).
   */
  $executeRaw<T = unknown>(query: TemplateStringsArray | Prisma.Sql, ...values: any[]): Prisma.PrismaPromise<number>;

  /**
   * Executes a raw query and returns the number of affected rows.
   * Susceptible to SQL injections, see documentation.
   * @example
   * ```
   * const result = await prisma.$executeRawUnsafe('UPDATE User SET cool = $1 WHERE email = $2 ;', true, 'user@email.com')
   * ```
   * 
   * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client/raw-database-access).
   */
  $executeRawUnsafe<T = unknown>(query: string, ...values: any[]): Prisma.PrismaPromise<number>;

  /**
   * Performs a prepared raw query and returns the `SELECT` data.
   * @example
   * ```
   * const result = await prisma.$queryRaw`SELECT * FROM User WHERE id = ${1} OR email = ${'user@email.com'};`
   * ```
   * 
   * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client/raw-database-access).
   */
  $queryRaw<T = unknown>(query: TemplateStringsArray | Prisma.Sql, ...values: any[]): Prisma.PrismaPromise<T>;

  /**
   * Performs a raw query and returns the `SELECT` data.
   * Susceptible to SQL injections, see documentation.
   * @example
   * ```
   * const result = await prisma.$queryRawUnsafe('SELECT * FROM User WHERE id = $1 OR email = $2;', 1, 'user@email.com')
   * ```
   * 
   * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client/raw-database-access).
   */
  $queryRawUnsafe<T = unknown>(query: string, ...values: any[]): Prisma.PrismaPromise<T>;

  /**
   * Allows the running of a sequence of read/write operations that are guaranteed to either succeed or fail as a whole.
   * @example
   * ```
   * const [george, bob, alice] = await prisma.$transaction([
   *   prisma.user.create({ data: { name: 'George' } }),
   *   prisma.user.create({ data: { name: 'Bob' } }),
   *   prisma.user.create({ data: { name: 'Alice' } }),
   * ])
   * ```
   * 
   * Read more in our [docs](https://www.prisma.io/docs/concepts/components/prisma-client/transactions).
   */
  $transaction<P extends Prisma.PrismaPromise<any>[]>(arg: [...P], options?: { isolationLevel?: Prisma.TransactionIsolationLevel }): $Utils.JsPromise<runtime.Types.Utils.UnwrapTuple<P>>

  $transaction<R>(fn: (prisma: Omit<PrismaClient, runtime.ITXClientDenyList>) => $Utils.JsPromise<R>, options?: { maxWait?: number, timeout?: number, isolationLevel?: Prisma.TransactionIsolationLevel }): $Utils.JsPromise<R>


  $extends: $Extensions.ExtendsHook<'extends', Prisma.TypeMapCb, ExtArgs>

      /**
   * `prisma.assignees`: Exposes CRUD operations for the **assignees** model.
    * Example usage:
    * ```ts
    * // Fetch zero or more Assignees
    * const assignees = await prisma.assignees.findMany()
    * ```
    */
  get assignees(): Prisma.assigneesDelegate<ExtArgs>;

  /**
   * `prisma.tasks`: Exposes CRUD operations for the **tasks** model.
    * Example usage:
    * ```ts
    * // Fetch zero or more Tasks
    * const tasks = await prisma.tasks.findMany()
    * ```
    */
  get tasks(): Prisma.tasksDelegate<ExtArgs>;

  /**
   * `prisma.users`: Exposes CRUD operations for the **users** model.
    * Example usage:
    * ```ts
    * // Fetch zero or more Users
    * const users = await prisma.users.findMany()
    * ```
    */
  get users(): Prisma.usersDelegate<ExtArgs>;
}

export namespace Prisma {
  export import DMMF = runtime.DMMF

  export type PrismaPromise<T> = $Public.PrismaPromise<T>

  /**
   * Validator
   */
  export import validator = runtime.Public.validator

  /**
   * Prisma Errors
   */
  export import PrismaClientKnownRequestError = runtime.PrismaClientKnownRequestError
  export import PrismaClientUnknownRequestError = runtime.PrismaClientUnknownRequestError
  export import PrismaClientRustPanicError = runtime.PrismaClientRustPanicError
  export import PrismaClientInitializationError = runtime.PrismaClientInitializationError
  export import PrismaClientValidationError = runtime.PrismaClientValidationError
  export import NotFoundError = runtime.NotFoundError

  /**
   * Re-export of sql-template-tag
   */
  export import sql = runtime.sqltag
  export import empty = runtime.empty
  export import join = runtime.join
  export import raw = runtime.raw
  export import Sql = runtime.Sql

  /**
   * Decimal.js
   */
  export import Decimal = runtime.Decimal

  export type DecimalJsLike = runtime.DecimalJsLike

  /**
   * Metrics 
   */
  export type Metrics = runtime.Metrics
  export type Metric<T> = runtime.Metric<T>
  export type MetricHistogram = runtime.MetricHistogram
  export type MetricHistogramBucket = runtime.MetricHistogramBucket

  /**
  * Extensions
  */
  export import Extension = $Extensions.UserArgs
  export import getExtensionContext = runtime.Extensions.getExtensionContext
  export import Args = $Public.Args
  export import Payload = $Public.Payload
  export import Result = $Public.Result
  export import Exact = $Public.Exact

  /**
   * Prisma Client JS version: 5.3.1
   * Query Engine version: 61e140623197a131c2a6189271ffee05a7aa9a59
   */
  export type PrismaVersion = {
    client: string
  }

  export const prismaVersion: PrismaVersion 

  /**
   * Utility Types
   */

  /**
   * From https://github.com/sindresorhus/type-fest/
   * Matches a JSON object.
   * This type can be useful to enforce some input to be JSON-compatible or as a super-type to be extended from. 
   */
  export type JsonObject = {[Key in string]?: JsonValue}

  /**
   * From https://github.com/sindresorhus/type-fest/
   * Matches a JSON array.
   */
  export interface JsonArray extends Array<JsonValue> {}

  /**
   * From https://github.com/sindresorhus/type-fest/
   * Matches any valid JSON value.
   */
  export type JsonValue = string | number | boolean | JsonObject | JsonArray | null

  /**
   * Matches a JSON object.
   * Unlike `JsonObject`, this type allows undefined and read-only properties.
   */
  export type InputJsonObject = {readonly [Key in string]?: InputJsonValue | null}

  /**
   * Matches a JSON array.
   * Unlike `JsonArray`, readonly arrays are assignable to this type.
   */
  export interface InputJsonArray extends ReadonlyArray<InputJsonValue | null> {}

  /**
   * Matches any valid value that can be used as an input for operations like
   * create and update as the value of a JSON field. Unlike `JsonValue`, this
   * type allows read-only arrays and read-only object properties and disallows
   * `null` at the top level.
   *
   * `null` cannot be used as the value of a JSON field because its meaning
   * would be ambiguous. Use `Prisma.JsonNull` to store the JSON null value or
   * `Prisma.DbNull` to clear the JSON value and set the field to the database
   * NULL value instead.
   *
   * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-by-null-values
   */
  export type InputJsonValue = string | number | boolean | InputJsonObject | InputJsonArray | { toJSON(): unknown }

  /**
   * Types of the values used to represent different kinds of `null` values when working with JSON fields.
   * 
   * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
   */
  namespace NullTypes {
    /**
    * Type of `Prisma.DbNull`.
    * 
    * You cannot use other instances of this class. Please use the `Prisma.DbNull` value.
    * 
    * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
    */
    class DbNull {
      private DbNull: never
      private constructor()
    }

    /**
    * Type of `Prisma.JsonNull`.
    * 
    * You cannot use other instances of this class. Please use the `Prisma.JsonNull` value.
    * 
    * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
    */
    class JsonNull {
      private JsonNull: never
      private constructor()
    }

    /**
    * Type of `Prisma.AnyNull`.
    * 
    * You cannot use other instances of this class. Please use the `Prisma.AnyNull` value.
    * 
    * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
    */
    class AnyNull {
      private AnyNull: never
      private constructor()
    }
  }

  /**
   * Helper for filtering JSON entries that have `null` on the database (empty on the db)
   * 
   * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
   */
  export const DbNull: NullTypes.DbNull

  /**
   * Helper for filtering JSON entries that have JSON `null` values (not empty on the db)
   * 
   * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
   */
  export const JsonNull: NullTypes.JsonNull

  /**
   * Helper for filtering JSON entries that are `Prisma.DbNull` or `Prisma.JsonNull`
   * 
   * @see https://www.prisma.io/docs/concepts/components/prisma-client/working-with-fields/working-with-json-fields#filtering-on-a-json-field
   */
  export const AnyNull: NullTypes.AnyNull

  type SelectAndInclude = {
    select: any
    include: any
  }

  /**
   * Get the type of the value, that the Promise holds.
   */
  export type PromiseType<T extends PromiseLike<any>> = T extends PromiseLike<infer U> ? U : T;

  /**
   * Get the return type of a function which returns a Promise.
   */
  export type PromiseReturnType<T extends (...args: any) => $Utils.JsPromise<any>> = PromiseType<ReturnType<T>>

  /**
   * From T, pick a set of properties whose keys are in the union K
   */
  type Prisma__Pick<T, K extends keyof T> = {
      [P in K]: T[P];
  };


  export type Enumerable<T> = T | Array<T>;

  export type RequiredKeys<T> = {
    [K in keyof T]-?: {} extends Prisma__Pick<T, K> ? never : K
  }[keyof T]

  export type TruthyKeys<T> = keyof {
    [K in keyof T as T[K] extends false | undefined | null ? never : K]: K
  }

  export type TrueKeys<T> = TruthyKeys<Prisma__Pick<T, RequiredKeys<T>>>

  /**
   * Subset
   * @desc From `T` pick properties that exist in `U`. Simple version of Intersection
   */
  export type Subset<T, U> = {
    [key in keyof T]: key extends keyof U ? T[key] : never;
  };

  /**
   * SelectSubset
   * @desc From `T` pick properties that exist in `U`. Simple version of Intersection.
   * Additionally, it validates, if both select and include are present. If the case, it errors.
   */
  export type SelectSubset<T, U> = {
    [key in keyof T]: key extends keyof U ? T[key] : never
  } &
    (T extends SelectAndInclude
      ? 'Please either choose `select` or `include`.'
      : {})

  /**
   * Subset + Intersection
   * @desc From `T` pick properties that exist in `U` and intersect `K`
   */
  export type SubsetIntersection<T, U, K> = {
    [key in keyof T]: key extends keyof U ? T[key] : never
  } &
    K

  type Without<T, U> = { [P in Exclude<keyof T, keyof U>]?: never };

  /**
   * XOR is needed to have a real mutually exclusive union type
   * https://stackoverflow.com/questions/42123407/does-typescript-support-mutually-exclusive-types
   */
  type XOR<T, U> =
    T extends object ?
    U extends object ?
      (Without<T, U> & U) | (Without<U, T> & T)
    : U : T


  /**
   * Is T a Record?
   */
  type IsObject<T extends any> = T extends Array<any>
  ? False
  : T extends Date
  ? False
  : T extends Uint8Array
  ? False
  : T extends BigInt
  ? False
  : T extends object
  ? True
  : False


  /**
   * If it's T[], return T
   */
  export type UnEnumerate<T extends unknown> = T extends Array<infer U> ? U : T

  /**
   * From ts-toolbelt
   */

  type __Either<O extends object, K extends Key> = Omit<O, K> &
    {
      // Merge all but K
      [P in K]: Prisma__Pick<O, P & keyof O> // With K possibilities
    }[K]

  type EitherStrict<O extends object, K extends Key> = Strict<__Either<O, K>>

  type EitherLoose<O extends object, K extends Key> = ComputeRaw<__Either<O, K>>

  type _Either<
    O extends object,
    K extends Key,
    strict extends Boolean
  > = {
    1: EitherStrict<O, K>
    0: EitherLoose<O, K>
  }[strict]

  type Either<
    O extends object,
    K extends Key,
    strict extends Boolean = 1
  > = O extends unknown ? _Either<O, K, strict> : never

  export type Union = any

  type PatchUndefined<O extends object, O1 extends object> = {
    [K in keyof O]: O[K] extends undefined ? At<O1, K> : O[K]
  } & {}

  /** Helper Types for "Merge" **/
  export type IntersectOf<U extends Union> = (
    U extends unknown ? (k: U) => void : never
  ) extends (k: infer I) => void
    ? I
    : never

  export type Overwrite<O extends object, O1 extends object> = {
      [K in keyof O]: K extends keyof O1 ? O1[K] : O[K];
  } & {};

  type _Merge<U extends object> = IntersectOf<Overwrite<U, {
      [K in keyof U]-?: At<U, K>;
  }>>;

  type Key = string | number | symbol;
  type AtBasic<O extends object, K extends Key> = K extends keyof O ? O[K] : never;
  type AtStrict<O extends object, K extends Key> = O[K & keyof O];
  type AtLoose<O extends object, K extends Key> = O extends unknown ? AtStrict<O, K> : never;
  export type At<O extends object, K extends Key, strict extends Boolean = 1> = {
      1: AtStrict<O, K>;
      0: AtLoose<O, K>;
  }[strict];

  export type ComputeRaw<A extends any> = A extends Function ? A : {
    [K in keyof A]: A[K];
  } & {};

  export type OptionalFlat<O> = {
    [K in keyof O]?: O[K];
  } & {};

  type _Record<K extends keyof any, T> = {
    [P in K]: T;
  };

  // cause typescript not to expand types and preserve names
  type NoExpand<T> = T extends unknown ? T : never;

  // this type assumes the passed object is entirely optional
  type AtLeast<O extends object, K extends string> = NoExpand<
    O extends unknown
    ? | (K extends keyof O ? { [P in K]: O[P] } & O : O)
      | {[P in keyof O as P extends K ? K : never]-?: O[P]} & O
    : never>;

  type _Strict<U, _U = U> = U extends unknown ? U & OptionalFlat<_Record<Exclude<Keys<_U>, keyof U>, never>> : never;

  export type Strict<U extends object> = ComputeRaw<_Strict<U>>;
  /** End Helper Types for "Merge" **/

  export type Merge<U extends object> = ComputeRaw<_Merge<Strict<U>>>;

  /**
  A [[Boolean]]
  */
  export type Boolean = True | False

  // /**
  // 1
  // */
  export type True = 1

  /**
  0
  */
  export type False = 0

  export type Not<B extends Boolean> = {
    0: 1
    1: 0
  }[B]

  export type Extends<A1 extends any, A2 extends any> = [A1] extends [never]
    ? 0 // anything `never` is false
    : A1 extends A2
    ? 1
    : 0

  export type Has<U extends Union, U1 extends Union> = Not<
    Extends<Exclude<U1, U>, U1>
  >

  export type Or<B1 extends Boolean, B2 extends Boolean> = {
    0: {
      0: 0
      1: 1
    }
    1: {
      0: 1
      1: 1
    }
  }[B1][B2]

  export type Keys<U extends Union> = U extends unknown ? keyof U : never

  type Cast<A, B> = A extends B ? A : B;

  export const type: unique symbol;



  /**
   * Used by group by
   */

  export type GetScalarType<T, O> = O extends object ? {
    [P in keyof T]: P extends keyof O
      ? O[P]
      : never
  } : never

  type FieldPaths<
    T,
    U = Omit<T, '_avg' | '_sum' | '_count' | '_min' | '_max'>
  > = IsObject<T> extends True ? U : T

  type GetHavingFields<T> = {
    [K in keyof T]: Or<
      Or<Extends<'OR', K>, Extends<'AND', K>>,
      Extends<'NOT', K>
    > extends True
      ? // infer is only needed to not hit TS limit
        // based on the brilliant idea of Pierre-Antoine Mills
        // https://github.com/microsoft/TypeScript/issues/30188#issuecomment-478938437
        T[K] extends infer TK
        ? GetHavingFields<UnEnumerate<TK> extends object ? Merge<UnEnumerate<TK>> : never>
        : never
      : {} extends FieldPaths<T[K]>
      ? never
      : K
  }[keyof T]

  /**
   * Convert tuple to union
   */
  type _TupleToUnion<T> = T extends (infer E)[] ? E : never
  type TupleToUnion<K extends readonly any[]> = _TupleToUnion<K>
  type MaybeTupleToUnion<T> = T extends any[] ? TupleToUnion<T> : T

  /**
   * Like `Pick`, but additionally can also accept an array of keys
   */
  type PickEnumerable<T, K extends Enumerable<keyof T> | keyof T> = Prisma__Pick<T, MaybeTupleToUnion<K>>

  /**
   * Exclude all keys with underscores
   */
  type ExcludeUnderscoreKeys<T extends string> = T extends `_${string}` ? never : T


  export type FieldRef<Model, FieldType> = runtime.FieldRef<Model, FieldType>

  type FieldRefInputType<Model, FieldType> = Model extends never ? never : FieldRef<Model, FieldType>


  export const ModelName: {
    assignees: 'assignees',
    tasks: 'tasks',
    users: 'users'
  };

  export type ModelName = (typeof ModelName)[keyof typeof ModelName]


  export type Datasources = {
    db?: Datasource
  }


  interface TypeMapCb extends $Utils.Fn<{extArgs: $Extensions.Args}, $Utils.Record<string, any>> {
    returns: Prisma.TypeMap<this['params']['extArgs']>
  }

  export type TypeMap<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    meta: {
      modelProps: 'assignees' | 'tasks' | 'users'
      txIsolationLevel: Prisma.TransactionIsolationLevel
    },
    model: {
      assignees: {
        payload: Prisma.$assigneesPayload<ExtArgs>
        fields: Prisma.assigneesFieldRefs
        operations: {
          findUnique: {
            args: Prisma.assigneesFindUniqueArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload> | null
          }
          findUniqueOrThrow: {
            args: Prisma.assigneesFindUniqueOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          findFirst: {
            args: Prisma.assigneesFindFirstArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload> | null
          }
          findFirstOrThrow: {
            args: Prisma.assigneesFindFirstOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          findMany: {
            args: Prisma.assigneesFindManyArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>[]
          }
          create: {
            args: Prisma.assigneesCreateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          createMany: {
            args: Prisma.assigneesCreateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          delete: {
            args: Prisma.assigneesDeleteArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          update: {
            args: Prisma.assigneesUpdateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          deleteMany: {
            args: Prisma.assigneesDeleteManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          updateMany: {
            args: Prisma.assigneesUpdateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          upsert: {
            args: Prisma.assigneesUpsertArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$assigneesPayload>
          }
          aggregate: {
            args: Prisma.AssigneesAggregateArgs<ExtArgs>,
            result: $Utils.Optional<AggregateAssignees>
          }
          groupBy: {
            args: Prisma.assigneesGroupByArgs<ExtArgs>,
            result: $Utils.Optional<AssigneesGroupByOutputType>[]
          }
          count: {
            args: Prisma.assigneesCountArgs<ExtArgs>,
            result: $Utils.Optional<AssigneesCountAggregateOutputType> | number
          }
        }
      }
      tasks: {
        payload: Prisma.$tasksPayload<ExtArgs>
        fields: Prisma.tasksFieldRefs
        operations: {
          findUnique: {
            args: Prisma.tasksFindUniqueArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload> | null
          }
          findUniqueOrThrow: {
            args: Prisma.tasksFindUniqueOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          findFirst: {
            args: Prisma.tasksFindFirstArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload> | null
          }
          findFirstOrThrow: {
            args: Prisma.tasksFindFirstOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          findMany: {
            args: Prisma.tasksFindManyArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>[]
          }
          create: {
            args: Prisma.tasksCreateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          createMany: {
            args: Prisma.tasksCreateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          delete: {
            args: Prisma.tasksDeleteArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          update: {
            args: Prisma.tasksUpdateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          deleteMany: {
            args: Prisma.tasksDeleteManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          updateMany: {
            args: Prisma.tasksUpdateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          upsert: {
            args: Prisma.tasksUpsertArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$tasksPayload>
          }
          aggregate: {
            args: Prisma.TasksAggregateArgs<ExtArgs>,
            result: $Utils.Optional<AggregateTasks>
          }
          groupBy: {
            args: Prisma.tasksGroupByArgs<ExtArgs>,
            result: $Utils.Optional<TasksGroupByOutputType>[]
          }
          count: {
            args: Prisma.tasksCountArgs<ExtArgs>,
            result: $Utils.Optional<TasksCountAggregateOutputType> | number
          }
        }
      }
      users: {
        payload: Prisma.$usersPayload<ExtArgs>
        fields: Prisma.usersFieldRefs
        operations: {
          findUnique: {
            args: Prisma.usersFindUniqueArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload> | null
          }
          findUniqueOrThrow: {
            args: Prisma.usersFindUniqueOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          findFirst: {
            args: Prisma.usersFindFirstArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload> | null
          }
          findFirstOrThrow: {
            args: Prisma.usersFindFirstOrThrowArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          findMany: {
            args: Prisma.usersFindManyArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>[]
          }
          create: {
            args: Prisma.usersCreateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          createMany: {
            args: Prisma.usersCreateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          delete: {
            args: Prisma.usersDeleteArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          update: {
            args: Prisma.usersUpdateArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          deleteMany: {
            args: Prisma.usersDeleteManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          updateMany: {
            args: Prisma.usersUpdateManyArgs<ExtArgs>,
            result: Prisma.BatchPayload
          }
          upsert: {
            args: Prisma.usersUpsertArgs<ExtArgs>,
            result: $Utils.PayloadToResult<Prisma.$usersPayload>
          }
          aggregate: {
            args: Prisma.UsersAggregateArgs<ExtArgs>,
            result: $Utils.Optional<AggregateUsers>
          }
          groupBy: {
            args: Prisma.usersGroupByArgs<ExtArgs>,
            result: $Utils.Optional<UsersGroupByOutputType>[]
          }
          count: {
            args: Prisma.usersCountArgs<ExtArgs>,
            result: $Utils.Optional<UsersCountAggregateOutputType> | number
          }
        }
      }
    }
  } & {
    other: {
      payload: any
      operations: {
        $executeRawUnsafe: {
          args: [query: string, ...values: any[]],
          result: any
        }
        $executeRaw: {
          args: [query: TemplateStringsArray | Prisma.Sql, ...values: any[]],
          result: any
        }
        $queryRawUnsafe: {
          args: [query: string, ...values: any[]],
          result: any
        }
        $queryRaw: {
          args: [query: TemplateStringsArray | Prisma.Sql, ...values: any[]],
          result: any
        }
      }
    }
  }
  export const defineExtension: $Extensions.ExtendsHook<'define', Prisma.TypeMapCb, $Extensions.DefaultArgs>
  export type DefaultPrismaClient = PrismaClient
  export type ErrorFormat = 'pretty' | 'colorless' | 'minimal'

  export interface PrismaClientOptions {
    /**
     * Overwrites the datasource url from your schema.prisma file
     */
    datasources?: Datasources

    /**
     * Overwrites the datasource url from your schema.prisma file
     */
    datasourceUrl?: string

    /**
     * @default "colorless"
     */
    errorFormat?: ErrorFormat

    /**
     * @example
     * ```
     * // Defaults to stdout
     * log: ['query', 'info', 'warn', 'error']
     * 
     * // Emit as events
     * log: [
     *  { emit: 'stdout', level: 'query' },
     *  { emit: 'stdout', level: 'info' },
     *  { emit: 'stdout', level: 'warn' }
     *  { emit: 'stdout', level: 'error' }
     * ]
     * ```
     * Read more in our [docs](https://www.prisma.io/docs/reference/tools-and-interfaces/prisma-client/logging#the-log-option).
     */
    log?: Array<LogLevel | LogDefinition>
  }

  /* Types for Logging */
  export type LogLevel = 'info' | 'query' | 'warn' | 'error'
  export type LogDefinition = {
    level: LogLevel
    emit: 'stdout' | 'event'
  }

  export type GetLogType<T extends LogLevel | LogDefinition> = T extends LogDefinition ? T['emit'] extends 'event' ? T['level'] : never : never
  export type GetEvents<T extends any> = T extends Array<LogLevel | LogDefinition> ?
    GetLogType<T[0]> | GetLogType<T[1]> | GetLogType<T[2]> | GetLogType<T[3]>
    : never

  export type QueryEvent = {
    timestamp: Date
    query: string
    params: string
    duration: number
    target: string
  }

  export type LogEvent = {
    timestamp: Date
    message: string
    target: string
  }
  /* End Types for Logging */


  export type PrismaAction =
    | 'findUnique'
    | 'findUniqueOrThrow'
    | 'findMany'
    | 'findFirst'
    | 'findFirstOrThrow'
    | 'create'
    | 'createMany'
    | 'update'
    | 'updateMany'
    | 'upsert'
    | 'delete'
    | 'deleteMany'
    | 'executeRaw'
    | 'queryRaw'
    | 'aggregate'
    | 'count'
    | 'runCommandRaw'
    | 'findRaw'
    | 'groupBy'

  /**
   * These options are being passed into the middleware as "params"
   */
  export type MiddlewareParams = {
    model?: ModelName
    action: PrismaAction
    args: any
    dataPath: string[]
    runInTransaction: boolean
  }

  /**
   * The `T` type makes sure, that the `return proceed` is not forgotten in the middleware implementation
   */
  export type Middleware<T = any> = (
    params: MiddlewareParams,
    next: (params: MiddlewareParams) => $Utils.JsPromise<T>,
  ) => $Utils.JsPromise<T>

  // tested in getLogLevel.test.ts
  export function getLogLevel(log: Array<LogLevel | LogDefinition>): LogLevel | undefined;

  /**
   * `PrismaClient` proxy available in interactive transactions.
   */
  export type TransactionClient = Omit<Prisma.DefaultPrismaClient, runtime.ITXClientDenyList>

  export type Datasource = {
    url?: string
  }

  /**
   * Count Types
   */


  /**
   * Count Type TasksCountOutputType
   */

  export type TasksCountOutputType = {
    assignees: number
  }

  export type TasksCountOutputTypeSelect<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    assignees?: boolean | TasksCountOutputTypeCountAssigneesArgs
  }

  // Custom InputTypes

  /**
   * TasksCountOutputType without action
   */
  export type TasksCountOutputTypeDefaultArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the TasksCountOutputType
     */
    select?: TasksCountOutputTypeSelect<ExtArgs> | null
  }


  /**
   * TasksCountOutputType without action
   */
  export type TasksCountOutputTypeCountAssigneesArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: assigneesWhereInput
  }



  /**
   * Count Type UsersCountOutputType
   */

  export type UsersCountOutputType = {
    assignees: number
    tasks: number
  }

  export type UsersCountOutputTypeSelect<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    assignees?: boolean | UsersCountOutputTypeCountAssigneesArgs
    tasks?: boolean | UsersCountOutputTypeCountTasksArgs
  }

  // Custom InputTypes

  /**
   * UsersCountOutputType without action
   */
  export type UsersCountOutputTypeDefaultArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the UsersCountOutputType
     */
    select?: UsersCountOutputTypeSelect<ExtArgs> | null
  }


  /**
   * UsersCountOutputType without action
   */
  export type UsersCountOutputTypeCountAssigneesArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: assigneesWhereInput
  }


  /**
   * UsersCountOutputType without action
   */
  export type UsersCountOutputTypeCountTasksArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: tasksWhereInput
  }



  /**
   * Models
   */

  /**
   * Model assignees
   */

  export type AggregateAssignees = {
    _count: AssigneesCountAggregateOutputType | null
    _avg: AssigneesAvgAggregateOutputType | null
    _sum: AssigneesSumAggregateOutputType | null
    _min: AssigneesMinAggregateOutputType | null
    _max: AssigneesMaxAggregateOutputType | null
  }

  export type AssigneesAvgAggregateOutputType = {
    userId: number | null
    taskId: number | null
  }

  export type AssigneesSumAggregateOutputType = {
    userId: number | null
    taskId: number | null
  }

  export type AssigneesMinAggregateOutputType = {
    userId: number | null
    taskId: number | null
  }

  export type AssigneesMaxAggregateOutputType = {
    userId: number | null
    taskId: number | null
  }

  export type AssigneesCountAggregateOutputType = {
    userId: number
    taskId: number
    _all: number
  }


  export type AssigneesAvgAggregateInputType = {
    userId?: true
    taskId?: true
  }

  export type AssigneesSumAggregateInputType = {
    userId?: true
    taskId?: true
  }

  export type AssigneesMinAggregateInputType = {
    userId?: true
    taskId?: true
  }

  export type AssigneesMaxAggregateInputType = {
    userId?: true
    taskId?: true
  }

  export type AssigneesCountAggregateInputType = {
    userId?: true
    taskId?: true
    _all?: true
  }

  export type AssigneesAggregateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which assignees to aggregate.
     */
    where?: assigneesWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of assignees to fetch.
     */
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the start position
     */
    cursor?: assigneesWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` assignees from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` assignees.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Count returned assignees
    **/
    _count?: true | AssigneesCountAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to average
    **/
    _avg?: AssigneesAvgAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to sum
    **/
    _sum?: AssigneesSumAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the minimum value
    **/
    _min?: AssigneesMinAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the maximum value
    **/
    _max?: AssigneesMaxAggregateInputType
  }

  export type GetAssigneesAggregateType<T extends AssigneesAggregateArgs> = {
        [P in keyof T & keyof AggregateAssignees]: P extends '_count' | 'count'
      ? T[P] extends true
        ? number
        : GetScalarType<T[P], AggregateAssignees[P]>
      : GetScalarType<T[P], AggregateAssignees[P]>
  }




  export type assigneesGroupByArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: assigneesWhereInput
    orderBy?: assigneesOrderByWithAggregationInput | assigneesOrderByWithAggregationInput[]
    by: AssigneesScalarFieldEnum[] | AssigneesScalarFieldEnum
    having?: assigneesScalarWhereWithAggregatesInput
    take?: number
    skip?: number
    _count?: AssigneesCountAggregateInputType | true
    _avg?: AssigneesAvgAggregateInputType
    _sum?: AssigneesSumAggregateInputType
    _min?: AssigneesMinAggregateInputType
    _max?: AssigneesMaxAggregateInputType
  }

  export type AssigneesGroupByOutputType = {
    userId: number
    taskId: number
    _count: AssigneesCountAggregateOutputType | null
    _avg: AssigneesAvgAggregateOutputType | null
    _sum: AssigneesSumAggregateOutputType | null
    _min: AssigneesMinAggregateOutputType | null
    _max: AssigneesMaxAggregateOutputType | null
  }

  type GetAssigneesGroupByPayload<T extends assigneesGroupByArgs> = Prisma.PrismaPromise<
    Array<
      PickEnumerable<AssigneesGroupByOutputType, T['by']> &
        {
          [P in ((keyof T) & (keyof AssigneesGroupByOutputType))]: P extends '_count'
            ? T[P] extends boolean
              ? number
              : GetScalarType<T[P], AssigneesGroupByOutputType[P]>
            : GetScalarType<T[P], AssigneesGroupByOutputType[P]>
        }
      >
    >


  export type assigneesSelect<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = $Extensions.GetSelect<{
    userId?: boolean
    taskId?: boolean
    tasks?: boolean | tasksDefaultArgs<ExtArgs>
    users?: boolean | usersDefaultArgs<ExtArgs>
  }, ExtArgs["result"]["assignees"]>

  export type assigneesSelectScalar = {
    userId?: boolean
    taskId?: boolean
  }

  export type assigneesInclude<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    tasks?: boolean | tasksDefaultArgs<ExtArgs>
    users?: boolean | usersDefaultArgs<ExtArgs>
  }


  export type $assigneesPayload<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    name: "assignees"
    objects: {
      tasks: Prisma.$tasksPayload<ExtArgs>
      users: Prisma.$usersPayload<ExtArgs>
    }
    scalars: $Extensions.GetResult<{
      userId: number
      taskId: number
    }, ExtArgs["result"]["assignees"]>
    composites: {}
  }


  type assigneesGetPayload<S extends boolean | null | undefined | assigneesDefaultArgs> = $Result.GetResult<Prisma.$assigneesPayload, S>

  type assigneesCountArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = 
    Omit<assigneesFindManyArgs, 'select' | 'include'> & {
      select?: AssigneesCountAggregateInputType | true
    }

  export interface assigneesDelegate<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> {
    [K: symbol]: { types: Prisma.TypeMap<ExtArgs>['model']['assignees'], meta: { name: 'assignees' } }
    /**
     * Find zero or one Assignees that matches the filter.
     * @param {assigneesFindUniqueArgs} args - Arguments to find a Assignees
     * @example
     * // Get one Assignees
     * const assignees = await prisma.assignees.findUnique({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUnique<T extends assigneesFindUniqueArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesFindUniqueArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findUnique'> | null, null, ExtArgs>

    /**
     * Find one Assignees that matches the filter or throw an error  with `error.code='P2025'` 
     *     if no matches were found.
     * @param {assigneesFindUniqueOrThrowArgs} args - Arguments to find a Assignees
     * @example
     * // Get one Assignees
     * const assignees = await prisma.assignees.findUniqueOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUniqueOrThrow<T extends assigneesFindUniqueOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesFindUniqueOrThrowArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findUniqueOrThrow'>, never, ExtArgs>

    /**
     * Find the first Assignees that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesFindFirstArgs} args - Arguments to find a Assignees
     * @example
     * // Get one Assignees
     * const assignees = await prisma.assignees.findFirst({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirst<T extends assigneesFindFirstArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesFindFirstArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findFirst'> | null, null, ExtArgs>

    /**
     * Find the first Assignees that matches the filter or
     * throw `PrismaKnownClientError` with `P2025` code if no matches were found.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesFindFirstOrThrowArgs} args - Arguments to find a Assignees
     * @example
     * // Get one Assignees
     * const assignees = await prisma.assignees.findFirstOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirstOrThrow<T extends assigneesFindFirstOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesFindFirstOrThrowArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findFirstOrThrow'>, never, ExtArgs>

    /**
     * Find zero or more Assignees that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesFindManyArgs=} args - Arguments to filter and select certain fields only.
     * @example
     * // Get all Assignees
     * const assignees = await prisma.assignees.findMany()
     * 
     * // Get first 10 Assignees
     * const assignees = await prisma.assignees.findMany({ take: 10 })
     * 
     * // Only select the `userId`
     * const assigneesWithUserIdOnly = await prisma.assignees.findMany({ select: { userId: true } })
     * 
    **/
    findMany<T extends assigneesFindManyArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesFindManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findMany'>>

    /**
     * Create a Assignees.
     * @param {assigneesCreateArgs} args - Arguments to create a Assignees.
     * @example
     * // Create one Assignees
     * const Assignees = await prisma.assignees.create({
     *   data: {
     *     // ... data to create a Assignees
     *   }
     * })
     * 
    **/
    create<T extends assigneesCreateArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesCreateArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'create'>, never, ExtArgs>

    /**
     * Create many Assignees.
     *     @param {assigneesCreateManyArgs} args - Arguments to create many Assignees.
     *     @example
     *     // Create many Assignees
     *     const assignees = await prisma.assignees.createMany({
     *       data: {
     *         // ... provide data here
     *       }
     *     })
     *     
    **/
    createMany<T extends assigneesCreateManyArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesCreateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Delete a Assignees.
     * @param {assigneesDeleteArgs} args - Arguments to delete one Assignees.
     * @example
     * // Delete one Assignees
     * const Assignees = await prisma.assignees.delete({
     *   where: {
     *     // ... filter to delete one Assignees
     *   }
     * })
     * 
    **/
    delete<T extends assigneesDeleteArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesDeleteArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'delete'>, never, ExtArgs>

    /**
     * Update one Assignees.
     * @param {assigneesUpdateArgs} args - Arguments to update one Assignees.
     * @example
     * // Update one Assignees
     * const assignees = await prisma.assignees.update({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    update<T extends assigneesUpdateArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesUpdateArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'update'>, never, ExtArgs>

    /**
     * Delete zero or more Assignees.
     * @param {assigneesDeleteManyArgs} args - Arguments to filter Assignees to delete.
     * @example
     * // Delete a few Assignees
     * const { count } = await prisma.assignees.deleteMany({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
     * 
    **/
    deleteMany<T extends assigneesDeleteManyArgs<ExtArgs>>(
      args?: SelectSubset<T, assigneesDeleteManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Update zero or more Assignees.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesUpdateManyArgs} args - Arguments to update one or more rows.
     * @example
     * // Update many Assignees
     * const assignees = await prisma.assignees.updateMany({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    updateMany<T extends assigneesUpdateManyArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesUpdateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Create or update one Assignees.
     * @param {assigneesUpsertArgs} args - Arguments to update or create a Assignees.
     * @example
     * // Update or create a Assignees
     * const assignees = await prisma.assignees.upsert({
     *   create: {
     *     // ... data to create a Assignees
     *   },
     *   update: {
     *     // ... in case it already exists, update
     *   },
     *   where: {
     *     // ... the filter for the Assignees we want to update
     *   }
     * })
    **/
    upsert<T extends assigneesUpsertArgs<ExtArgs>>(
      args: SelectSubset<T, assigneesUpsertArgs<ExtArgs>>
    ): Prisma__assigneesClient<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'upsert'>, never, ExtArgs>

    /**
     * Count the number of Assignees.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesCountArgs} args - Arguments to filter Assignees to count.
     * @example
     * // Count the number of Assignees
     * const count = await prisma.assignees.count({
     *   where: {
     *     // ... the filter for the Assignees we want to count
     *   }
     * })
    **/
    count<T extends assigneesCountArgs>(
      args?: Subset<T, assigneesCountArgs>,
    ): Prisma.PrismaPromise<
      T extends $Utils.Record<'select', any>
        ? T['select'] extends true
          ? number
          : GetScalarType<T['select'], AssigneesCountAggregateOutputType>
        : number
    >

    /**
     * Allows you to perform aggregations operations on a Assignees.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {AssigneesAggregateArgs} args - Select which aggregations you would like to apply and on what fields.
     * @example
     * // Ordered by age ascending
     * // Where email contains prisma.io
     * // Limited to the 10 users
     * const aggregations = await prisma.user.aggregate({
     *   _avg: {
     *     age: true,
     *   },
     *   where: {
     *     email: {
     *       contains: "prisma.io",
     *     },
     *   },
     *   orderBy: {
     *     age: "asc",
     *   },
     *   take: 10,
     * })
    **/
    aggregate<T extends AssigneesAggregateArgs>(args: Subset<T, AssigneesAggregateArgs>): Prisma.PrismaPromise<GetAssigneesAggregateType<T>>

    /**
     * Group by Assignees.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {assigneesGroupByArgs} args - Group by arguments.
     * @example
     * // Group by city, order by createdAt, get count
     * const result = await prisma.user.groupBy({
     *   by: ['city', 'createdAt'],
     *   orderBy: {
     *     createdAt: true
     *   },
     *   _count: {
     *     _all: true
     *   },
     * })
     * 
    **/
    groupBy<
      T extends assigneesGroupByArgs,
      HasSelectOrTake extends Or<
        Extends<'skip', Keys<T>>,
        Extends<'take', Keys<T>>
      >,
      OrderByArg extends True extends HasSelectOrTake
        ? { orderBy: assigneesGroupByArgs['orderBy'] }
        : { orderBy?: assigneesGroupByArgs['orderBy'] },
      OrderFields extends ExcludeUnderscoreKeys<Keys<MaybeTupleToUnion<T['orderBy']>>>,
      ByFields extends MaybeTupleToUnion<T['by']>,
      ByValid extends Has<ByFields, OrderFields>,
      HavingFields extends GetHavingFields<T['having']>,
      HavingValid extends Has<ByFields, HavingFields>,
      ByEmpty extends T['by'] extends never[] ? True : False,
      InputErrors extends ByEmpty extends True
      ? `Error: "by" must not be empty.`
      : HavingValid extends False
      ? {
          [P in HavingFields]: P extends ByFields
            ? never
            : P extends string
            ? `Error: Field "${P}" used in "having" needs to be provided in "by".`
            : [
                Error,
                'Field ',
                P,
                ` in "having" needs to be provided in "by"`,
              ]
        }[HavingFields]
      : 'take' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "take", you also need to provide "orderBy"'
      : 'skip' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "skip", you also need to provide "orderBy"'
      : ByValid extends True
      ? {}
      : {
          [P in OrderFields]: P extends ByFields
            ? never
            : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
        }[OrderFields]
    >(args: SubsetIntersection<T, assigneesGroupByArgs, OrderByArg> & InputErrors): {} extends InputErrors ? GetAssigneesGroupByPayload<T> : Prisma.PrismaPromise<InputErrors>
  /**
   * Fields of the assignees model
   */
  readonly fields: assigneesFieldRefs;
  }

  /**
   * The delegate class that acts as a "Promise-like" for assignees.
   * Why is this prefixed with `Prisma__`?
   * Because we want to prevent naming conflicts as mentioned in
   * https://github.com/prisma/prisma-client-js/issues/707
   */
  export interface Prisma__assigneesClient<T, Null = never, ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> extends Prisma.PrismaPromise<T> {
    readonly [Symbol.toStringTag]: 'PrismaPromise';

    tasks<T extends tasksDefaultArgs<ExtArgs> = {}>(args?: Subset<T, tasksDefaultArgs<ExtArgs>>): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findUniqueOrThrow'> | Null, Null, ExtArgs>;

    users<T extends usersDefaultArgs<ExtArgs> = {}>(args?: Subset<T, usersDefaultArgs<ExtArgs>>): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findUniqueOrThrow'> | Null, Null, ExtArgs>;

    /**
     * Attaches callbacks for the resolution and/or rejection of the Promise.
     * @param onfulfilled The callback to execute when the Promise is resolved.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of which ever callback is executed.
     */
    then<TResult1 = T, TResult2 = never>(onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | undefined | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null): $Utils.JsPromise<TResult1 | TResult2>;
    /**
     * Attaches a callback for only the rejection of the Promise.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of the callback.
     */
    catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null): $Utils.JsPromise<T | TResult>;
    /**
     * Attaches a callback that is invoked when the Promise is settled (fulfilled or rejected). The
     * resolved value cannot be modified from the callback.
     * @param onfinally The callback to execute when the Promise is settled (fulfilled or rejected).
     * @returns A Promise for the completion of the callback.
     */
    finally(onfinally?: (() => void) | undefined | null): $Utils.JsPromise<T>;
  }



  /**
   * Fields of the assignees model
   */ 
  interface assigneesFieldRefs {
    readonly userId: FieldRef<"assignees", 'Int'>
    readonly taskId: FieldRef<"assignees", 'Int'>
  }
    

  // Custom InputTypes

  /**
   * assignees findUnique
   */
  export type assigneesFindUniqueArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter, which assignees to fetch.
     */
    where: assigneesWhereUniqueInput
  }


  /**
   * assignees findUniqueOrThrow
   */
  export type assigneesFindUniqueOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter, which assignees to fetch.
     */
    where: assigneesWhereUniqueInput
  }


  /**
   * assignees findFirst
   */
  export type assigneesFindFirstArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter, which assignees to fetch.
     */
    where?: assigneesWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of assignees to fetch.
     */
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for assignees.
     */
    cursor?: assigneesWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` assignees from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` assignees.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of assignees.
     */
    distinct?: AssigneesScalarFieldEnum | AssigneesScalarFieldEnum[]
  }


  /**
   * assignees findFirstOrThrow
   */
  export type assigneesFindFirstOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter, which assignees to fetch.
     */
    where?: assigneesWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of assignees to fetch.
     */
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for assignees.
     */
    cursor?: assigneesWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` assignees from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` assignees.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of assignees.
     */
    distinct?: AssigneesScalarFieldEnum | AssigneesScalarFieldEnum[]
  }


  /**
   * assignees findMany
   */
  export type assigneesFindManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter, which assignees to fetch.
     */
    where?: assigneesWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of assignees to fetch.
     */
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for listing assignees.
     */
    cursor?: assigneesWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` assignees from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` assignees.
     */
    skip?: number
    distinct?: AssigneesScalarFieldEnum | AssigneesScalarFieldEnum[]
  }


  /**
   * assignees create
   */
  export type assigneesCreateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * The data needed to create a assignees.
     */
    data: XOR<assigneesCreateInput, assigneesUncheckedCreateInput>
  }


  /**
   * assignees createMany
   */
  export type assigneesCreateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to create many assignees.
     */
    data: assigneesCreateManyInput | assigneesCreateManyInput[]
    skipDuplicates?: boolean
  }


  /**
   * assignees update
   */
  export type assigneesUpdateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * The data needed to update a assignees.
     */
    data: XOR<assigneesUpdateInput, assigneesUncheckedUpdateInput>
    /**
     * Choose, which assignees to update.
     */
    where: assigneesWhereUniqueInput
  }


  /**
   * assignees updateMany
   */
  export type assigneesUpdateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to update assignees.
     */
    data: XOR<assigneesUpdateManyMutationInput, assigneesUncheckedUpdateManyInput>
    /**
     * Filter which assignees to update
     */
    where?: assigneesWhereInput
  }


  /**
   * assignees upsert
   */
  export type assigneesUpsertArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * The filter to search for the assignees to update in case it exists.
     */
    where: assigneesWhereUniqueInput
    /**
     * In case the assignees found by the `where` argument doesn't exist, create a new assignees with this data.
     */
    create: XOR<assigneesCreateInput, assigneesUncheckedCreateInput>
    /**
     * In case the assignees was found with the provided `where` argument, update it with this data.
     */
    update: XOR<assigneesUpdateInput, assigneesUncheckedUpdateInput>
  }


  /**
   * assignees delete
   */
  export type assigneesDeleteArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    /**
     * Filter which assignees to delete.
     */
    where: assigneesWhereUniqueInput
  }


  /**
   * assignees deleteMany
   */
  export type assigneesDeleteManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which assignees to delete
     */
    where?: assigneesWhereInput
  }


  /**
   * assignees without action
   */
  export type assigneesDefaultArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
  }



  /**
   * Model tasks
   */

  export type AggregateTasks = {
    _count: TasksCountAggregateOutputType | null
    _avg: TasksAvgAggregateOutputType | null
    _sum: TasksSumAggregateOutputType | null
    _min: TasksMinAggregateOutputType | null
    _max: TasksMaxAggregateOutputType | null
  }

  export type TasksAvgAggregateOutputType = {
    id: number | null
    creatorid: number | null
  }

  export type TasksSumAggregateOutputType = {
    id: number | null
    creatorid: number | null
  }

  export type TasksMinAggregateOutputType = {
    id: number | null
    title: string | null
    created_at: Date | null
    due_at: Date | null
    creatorid: number | null
    isDone: boolean | null
  }

  export type TasksMaxAggregateOutputType = {
    id: number | null
    title: string | null
    created_at: Date | null
    due_at: Date | null
    creatorid: number | null
    isDone: boolean | null
  }

  export type TasksCountAggregateOutputType = {
    id: number
    title: number
    created_at: number
    due_at: number
    creatorid: number
    isDone: number
    _all: number
  }


  export type TasksAvgAggregateInputType = {
    id?: true
    creatorid?: true
  }

  export type TasksSumAggregateInputType = {
    id?: true
    creatorid?: true
  }

  export type TasksMinAggregateInputType = {
    id?: true
    title?: true
    created_at?: true
    due_at?: true
    creatorid?: true
    isDone?: true
  }

  export type TasksMaxAggregateInputType = {
    id?: true
    title?: true
    created_at?: true
    due_at?: true
    creatorid?: true
    isDone?: true
  }

  export type TasksCountAggregateInputType = {
    id?: true
    title?: true
    created_at?: true
    due_at?: true
    creatorid?: true
    isDone?: true
    _all?: true
  }

  export type TasksAggregateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which tasks to aggregate.
     */
    where?: tasksWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of tasks to fetch.
     */
    orderBy?: tasksOrderByWithRelationInput | tasksOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the start position
     */
    cursor?: tasksWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` tasks from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` tasks.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Count returned tasks
    **/
    _count?: true | TasksCountAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to average
    **/
    _avg?: TasksAvgAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to sum
    **/
    _sum?: TasksSumAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the minimum value
    **/
    _min?: TasksMinAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the maximum value
    **/
    _max?: TasksMaxAggregateInputType
  }

  export type GetTasksAggregateType<T extends TasksAggregateArgs> = {
        [P in keyof T & keyof AggregateTasks]: P extends '_count' | 'count'
      ? T[P] extends true
        ? number
        : GetScalarType<T[P], AggregateTasks[P]>
      : GetScalarType<T[P], AggregateTasks[P]>
  }




  export type tasksGroupByArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: tasksWhereInput
    orderBy?: tasksOrderByWithAggregationInput | tasksOrderByWithAggregationInput[]
    by: TasksScalarFieldEnum[] | TasksScalarFieldEnum
    having?: tasksScalarWhereWithAggregatesInput
    take?: number
    skip?: number
    _count?: TasksCountAggregateInputType | true
    _avg?: TasksAvgAggregateInputType
    _sum?: TasksSumAggregateInputType
    _min?: TasksMinAggregateInputType
    _max?: TasksMaxAggregateInputType
  }

  export type TasksGroupByOutputType = {
    id: number
    title: string | null
    created_at: Date | null
    due_at: Date | null
    creatorid: number
    isDone: boolean
    _count: TasksCountAggregateOutputType | null
    _avg: TasksAvgAggregateOutputType | null
    _sum: TasksSumAggregateOutputType | null
    _min: TasksMinAggregateOutputType | null
    _max: TasksMaxAggregateOutputType | null
  }

  type GetTasksGroupByPayload<T extends tasksGroupByArgs> = Prisma.PrismaPromise<
    Array<
      PickEnumerable<TasksGroupByOutputType, T['by']> &
        {
          [P in ((keyof T) & (keyof TasksGroupByOutputType))]: P extends '_count'
            ? T[P] extends boolean
              ? number
              : GetScalarType<T[P], TasksGroupByOutputType[P]>
            : GetScalarType<T[P], TasksGroupByOutputType[P]>
        }
      >
    >


  export type tasksSelect<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = $Extensions.GetSelect<{
    id?: boolean
    title?: boolean
    created_at?: boolean
    due_at?: boolean
    creatorid?: boolean
    isDone?: boolean
    assignees?: boolean | tasks$assigneesArgs<ExtArgs>
    creator?: boolean | usersDefaultArgs<ExtArgs>
    _count?: boolean | TasksCountOutputTypeDefaultArgs<ExtArgs>
  }, ExtArgs["result"]["tasks"]>

  export type tasksSelectScalar = {
    id?: boolean
    title?: boolean
    created_at?: boolean
    due_at?: boolean
    creatorid?: boolean
    isDone?: boolean
  }

  export type tasksInclude<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    assignees?: boolean | tasks$assigneesArgs<ExtArgs>
    creator?: boolean | usersDefaultArgs<ExtArgs>
    _count?: boolean | TasksCountOutputTypeDefaultArgs<ExtArgs>
  }


  export type $tasksPayload<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    name: "tasks"
    objects: {
      assignees: Prisma.$assigneesPayload<ExtArgs>[]
      creator: Prisma.$usersPayload<ExtArgs>
    }
    scalars: $Extensions.GetResult<{
      id: number
      title: string | null
      created_at: Date | null
      due_at: Date | null
      creatorid: number
      isDone: boolean
    }, ExtArgs["result"]["tasks"]>
    composites: {}
  }


  type tasksGetPayload<S extends boolean | null | undefined | tasksDefaultArgs> = $Result.GetResult<Prisma.$tasksPayload, S>

  type tasksCountArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = 
    Omit<tasksFindManyArgs, 'select' | 'include'> & {
      select?: TasksCountAggregateInputType | true
    }

  export interface tasksDelegate<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> {
    [K: symbol]: { types: Prisma.TypeMap<ExtArgs>['model']['tasks'], meta: { name: 'tasks' } }
    /**
     * Find zero or one Tasks that matches the filter.
     * @param {tasksFindUniqueArgs} args - Arguments to find a Tasks
     * @example
     * // Get one Tasks
     * const tasks = await prisma.tasks.findUnique({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUnique<T extends tasksFindUniqueArgs<ExtArgs>>(
      args: SelectSubset<T, tasksFindUniqueArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findUnique'> | null, null, ExtArgs>

    /**
     * Find one Tasks that matches the filter or throw an error  with `error.code='P2025'` 
     *     if no matches were found.
     * @param {tasksFindUniqueOrThrowArgs} args - Arguments to find a Tasks
     * @example
     * // Get one Tasks
     * const tasks = await prisma.tasks.findUniqueOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUniqueOrThrow<T extends tasksFindUniqueOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksFindUniqueOrThrowArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findUniqueOrThrow'>, never, ExtArgs>

    /**
     * Find the first Tasks that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksFindFirstArgs} args - Arguments to find a Tasks
     * @example
     * // Get one Tasks
     * const tasks = await prisma.tasks.findFirst({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirst<T extends tasksFindFirstArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksFindFirstArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findFirst'> | null, null, ExtArgs>

    /**
     * Find the first Tasks that matches the filter or
     * throw `PrismaKnownClientError` with `P2025` code if no matches were found.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksFindFirstOrThrowArgs} args - Arguments to find a Tasks
     * @example
     * // Get one Tasks
     * const tasks = await prisma.tasks.findFirstOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirstOrThrow<T extends tasksFindFirstOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksFindFirstOrThrowArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findFirstOrThrow'>, never, ExtArgs>

    /**
     * Find zero or more Tasks that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksFindManyArgs=} args - Arguments to filter and select certain fields only.
     * @example
     * // Get all Tasks
     * const tasks = await prisma.tasks.findMany()
     * 
     * // Get first 10 Tasks
     * const tasks = await prisma.tasks.findMany({ take: 10 })
     * 
     * // Only select the `id`
     * const tasksWithIdOnly = await prisma.tasks.findMany({ select: { id: true } })
     * 
    **/
    findMany<T extends tasksFindManyArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksFindManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findMany'>>

    /**
     * Create a Tasks.
     * @param {tasksCreateArgs} args - Arguments to create a Tasks.
     * @example
     * // Create one Tasks
     * const Tasks = await prisma.tasks.create({
     *   data: {
     *     // ... data to create a Tasks
     *   }
     * })
     * 
    **/
    create<T extends tasksCreateArgs<ExtArgs>>(
      args: SelectSubset<T, tasksCreateArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'create'>, never, ExtArgs>

    /**
     * Create many Tasks.
     *     @param {tasksCreateManyArgs} args - Arguments to create many Tasks.
     *     @example
     *     // Create many Tasks
     *     const tasks = await prisma.tasks.createMany({
     *       data: {
     *         // ... provide data here
     *       }
     *     })
     *     
    **/
    createMany<T extends tasksCreateManyArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksCreateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Delete a Tasks.
     * @param {tasksDeleteArgs} args - Arguments to delete one Tasks.
     * @example
     * // Delete one Tasks
     * const Tasks = await prisma.tasks.delete({
     *   where: {
     *     // ... filter to delete one Tasks
     *   }
     * })
     * 
    **/
    delete<T extends tasksDeleteArgs<ExtArgs>>(
      args: SelectSubset<T, tasksDeleteArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'delete'>, never, ExtArgs>

    /**
     * Update one Tasks.
     * @param {tasksUpdateArgs} args - Arguments to update one Tasks.
     * @example
     * // Update one Tasks
     * const tasks = await prisma.tasks.update({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    update<T extends tasksUpdateArgs<ExtArgs>>(
      args: SelectSubset<T, tasksUpdateArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'update'>, never, ExtArgs>

    /**
     * Delete zero or more Tasks.
     * @param {tasksDeleteManyArgs} args - Arguments to filter Tasks to delete.
     * @example
     * // Delete a few Tasks
     * const { count } = await prisma.tasks.deleteMany({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
     * 
    **/
    deleteMany<T extends tasksDeleteManyArgs<ExtArgs>>(
      args?: SelectSubset<T, tasksDeleteManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Update zero or more Tasks.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksUpdateManyArgs} args - Arguments to update one or more rows.
     * @example
     * // Update many Tasks
     * const tasks = await prisma.tasks.updateMany({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    updateMany<T extends tasksUpdateManyArgs<ExtArgs>>(
      args: SelectSubset<T, tasksUpdateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Create or update one Tasks.
     * @param {tasksUpsertArgs} args - Arguments to update or create a Tasks.
     * @example
     * // Update or create a Tasks
     * const tasks = await prisma.tasks.upsert({
     *   create: {
     *     // ... data to create a Tasks
     *   },
     *   update: {
     *     // ... in case it already exists, update
     *   },
     *   where: {
     *     // ... the filter for the Tasks we want to update
     *   }
     * })
    **/
    upsert<T extends tasksUpsertArgs<ExtArgs>>(
      args: SelectSubset<T, tasksUpsertArgs<ExtArgs>>
    ): Prisma__tasksClient<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'upsert'>, never, ExtArgs>

    /**
     * Count the number of Tasks.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksCountArgs} args - Arguments to filter Tasks to count.
     * @example
     * // Count the number of Tasks
     * const count = await prisma.tasks.count({
     *   where: {
     *     // ... the filter for the Tasks we want to count
     *   }
     * })
    **/
    count<T extends tasksCountArgs>(
      args?: Subset<T, tasksCountArgs>,
    ): Prisma.PrismaPromise<
      T extends $Utils.Record<'select', any>
        ? T['select'] extends true
          ? number
          : GetScalarType<T['select'], TasksCountAggregateOutputType>
        : number
    >

    /**
     * Allows you to perform aggregations operations on a Tasks.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {TasksAggregateArgs} args - Select which aggregations you would like to apply and on what fields.
     * @example
     * // Ordered by age ascending
     * // Where email contains prisma.io
     * // Limited to the 10 users
     * const aggregations = await prisma.user.aggregate({
     *   _avg: {
     *     age: true,
     *   },
     *   where: {
     *     email: {
     *       contains: "prisma.io",
     *     },
     *   },
     *   orderBy: {
     *     age: "asc",
     *   },
     *   take: 10,
     * })
    **/
    aggregate<T extends TasksAggregateArgs>(args: Subset<T, TasksAggregateArgs>): Prisma.PrismaPromise<GetTasksAggregateType<T>>

    /**
     * Group by Tasks.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {tasksGroupByArgs} args - Group by arguments.
     * @example
     * // Group by city, order by createdAt, get count
     * const result = await prisma.user.groupBy({
     *   by: ['city', 'createdAt'],
     *   orderBy: {
     *     createdAt: true
     *   },
     *   _count: {
     *     _all: true
     *   },
     * })
     * 
    **/
    groupBy<
      T extends tasksGroupByArgs,
      HasSelectOrTake extends Or<
        Extends<'skip', Keys<T>>,
        Extends<'take', Keys<T>>
      >,
      OrderByArg extends True extends HasSelectOrTake
        ? { orderBy: tasksGroupByArgs['orderBy'] }
        : { orderBy?: tasksGroupByArgs['orderBy'] },
      OrderFields extends ExcludeUnderscoreKeys<Keys<MaybeTupleToUnion<T['orderBy']>>>,
      ByFields extends MaybeTupleToUnion<T['by']>,
      ByValid extends Has<ByFields, OrderFields>,
      HavingFields extends GetHavingFields<T['having']>,
      HavingValid extends Has<ByFields, HavingFields>,
      ByEmpty extends T['by'] extends never[] ? True : False,
      InputErrors extends ByEmpty extends True
      ? `Error: "by" must not be empty.`
      : HavingValid extends False
      ? {
          [P in HavingFields]: P extends ByFields
            ? never
            : P extends string
            ? `Error: Field "${P}" used in "having" needs to be provided in "by".`
            : [
                Error,
                'Field ',
                P,
                ` in "having" needs to be provided in "by"`,
              ]
        }[HavingFields]
      : 'take' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "take", you also need to provide "orderBy"'
      : 'skip' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "skip", you also need to provide "orderBy"'
      : ByValid extends True
      ? {}
      : {
          [P in OrderFields]: P extends ByFields
            ? never
            : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
        }[OrderFields]
    >(args: SubsetIntersection<T, tasksGroupByArgs, OrderByArg> & InputErrors): {} extends InputErrors ? GetTasksGroupByPayload<T> : Prisma.PrismaPromise<InputErrors>
  /**
   * Fields of the tasks model
   */
  readonly fields: tasksFieldRefs;
  }

  /**
   * The delegate class that acts as a "Promise-like" for tasks.
   * Why is this prefixed with `Prisma__`?
   * Because we want to prevent naming conflicts as mentioned in
   * https://github.com/prisma/prisma-client-js/issues/707
   */
  export interface Prisma__tasksClient<T, Null = never, ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> extends Prisma.PrismaPromise<T> {
    readonly [Symbol.toStringTag]: 'PrismaPromise';

    assignees<T extends tasks$assigneesArgs<ExtArgs> = {}>(args?: Subset<T, tasks$assigneesArgs<ExtArgs>>): Prisma.PrismaPromise<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findMany'> | Null>;

    creator<T extends usersDefaultArgs<ExtArgs> = {}>(args?: Subset<T, usersDefaultArgs<ExtArgs>>): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findUniqueOrThrow'> | Null, Null, ExtArgs>;

    /**
     * Attaches callbacks for the resolution and/or rejection of the Promise.
     * @param onfulfilled The callback to execute when the Promise is resolved.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of which ever callback is executed.
     */
    then<TResult1 = T, TResult2 = never>(onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | undefined | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null): $Utils.JsPromise<TResult1 | TResult2>;
    /**
     * Attaches a callback for only the rejection of the Promise.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of the callback.
     */
    catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null): $Utils.JsPromise<T | TResult>;
    /**
     * Attaches a callback that is invoked when the Promise is settled (fulfilled or rejected). The
     * resolved value cannot be modified from the callback.
     * @param onfinally The callback to execute when the Promise is settled (fulfilled or rejected).
     * @returns A Promise for the completion of the callback.
     */
    finally(onfinally?: (() => void) | undefined | null): $Utils.JsPromise<T>;
  }



  /**
   * Fields of the tasks model
   */ 
  interface tasksFieldRefs {
    readonly id: FieldRef<"tasks", 'Int'>
    readonly title: FieldRef<"tasks", 'String'>
    readonly created_at: FieldRef<"tasks", 'DateTime'>
    readonly due_at: FieldRef<"tasks", 'DateTime'>
    readonly creatorid: FieldRef<"tasks", 'Int'>
    readonly isDone: FieldRef<"tasks", 'Boolean'>
  }
    

  // Custom InputTypes

  /**
   * tasks findUnique
   */
  export type tasksFindUniqueArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter, which tasks to fetch.
     */
    where: tasksWhereUniqueInput
  }


  /**
   * tasks findUniqueOrThrow
   */
  export type tasksFindUniqueOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter, which tasks to fetch.
     */
    where: tasksWhereUniqueInput
  }


  /**
   * tasks findFirst
   */
  export type tasksFindFirstArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter, which tasks to fetch.
     */
    where?: tasksWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of tasks to fetch.
     */
    orderBy?: tasksOrderByWithRelationInput | tasksOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for tasks.
     */
    cursor?: tasksWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` tasks from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` tasks.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of tasks.
     */
    distinct?: TasksScalarFieldEnum | TasksScalarFieldEnum[]
  }


  /**
   * tasks findFirstOrThrow
   */
  export type tasksFindFirstOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter, which tasks to fetch.
     */
    where?: tasksWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of tasks to fetch.
     */
    orderBy?: tasksOrderByWithRelationInput | tasksOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for tasks.
     */
    cursor?: tasksWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` tasks from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` tasks.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of tasks.
     */
    distinct?: TasksScalarFieldEnum | TasksScalarFieldEnum[]
  }


  /**
   * tasks findMany
   */
  export type tasksFindManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter, which tasks to fetch.
     */
    where?: tasksWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of tasks to fetch.
     */
    orderBy?: tasksOrderByWithRelationInput | tasksOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for listing tasks.
     */
    cursor?: tasksWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` tasks from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` tasks.
     */
    skip?: number
    distinct?: TasksScalarFieldEnum | TasksScalarFieldEnum[]
  }


  /**
   * tasks create
   */
  export type tasksCreateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * The data needed to create a tasks.
     */
    data: XOR<tasksCreateInput, tasksUncheckedCreateInput>
  }


  /**
   * tasks createMany
   */
  export type tasksCreateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to create many tasks.
     */
    data: tasksCreateManyInput | tasksCreateManyInput[]
    skipDuplicates?: boolean
  }


  /**
   * tasks update
   */
  export type tasksUpdateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * The data needed to update a tasks.
     */
    data: XOR<tasksUpdateInput, tasksUncheckedUpdateInput>
    /**
     * Choose, which tasks to update.
     */
    where: tasksWhereUniqueInput
  }


  /**
   * tasks updateMany
   */
  export type tasksUpdateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to update tasks.
     */
    data: XOR<tasksUpdateManyMutationInput, tasksUncheckedUpdateManyInput>
    /**
     * Filter which tasks to update
     */
    where?: tasksWhereInput
  }


  /**
   * tasks upsert
   */
  export type tasksUpsertArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * The filter to search for the tasks to update in case it exists.
     */
    where: tasksWhereUniqueInput
    /**
     * In case the tasks found by the `where` argument doesn't exist, create a new tasks with this data.
     */
    create: XOR<tasksCreateInput, tasksUncheckedCreateInput>
    /**
     * In case the tasks was found with the provided `where` argument, update it with this data.
     */
    update: XOR<tasksUpdateInput, tasksUncheckedUpdateInput>
  }


  /**
   * tasks delete
   */
  export type tasksDeleteArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    /**
     * Filter which tasks to delete.
     */
    where: tasksWhereUniqueInput
  }


  /**
   * tasks deleteMany
   */
  export type tasksDeleteManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which tasks to delete
     */
    where?: tasksWhereInput
  }


  /**
   * tasks.assignees
   */
  export type tasks$assigneesArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    where?: assigneesWhereInput
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    cursor?: assigneesWhereUniqueInput
    take?: number
    skip?: number
    distinct?: AssigneesScalarFieldEnum | AssigneesScalarFieldEnum[]
  }


  /**
   * tasks without action
   */
  export type tasksDefaultArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
  }



  /**
   * Model users
   */

  export type AggregateUsers = {
    _count: UsersCountAggregateOutputType | null
    _avg: UsersAvgAggregateOutputType | null
    _sum: UsersSumAggregateOutputType | null
    _min: UsersMinAggregateOutputType | null
    _max: UsersMaxAggregateOutputType | null
  }

  export type UsersAvgAggregateOutputType = {
    id: number | null
  }

  export type UsersSumAggregateOutputType = {
    id: number | null
  }

  export type UsersMinAggregateOutputType = {
    id: number | null
    name: string | null
    image: string | null
    password: string | null
    refreshToken: string | null
    username: string | null
  }

  export type UsersMaxAggregateOutputType = {
    id: number | null
    name: string | null
    image: string | null
    password: string | null
    refreshToken: string | null
    username: string | null
  }

  export type UsersCountAggregateOutputType = {
    id: number
    name: number
    image: number
    password: number
    refreshToken: number
    username: number
    _all: number
  }


  export type UsersAvgAggregateInputType = {
    id?: true
  }

  export type UsersSumAggregateInputType = {
    id?: true
  }

  export type UsersMinAggregateInputType = {
    id?: true
    name?: true
    image?: true
    password?: true
    refreshToken?: true
    username?: true
  }

  export type UsersMaxAggregateInputType = {
    id?: true
    name?: true
    image?: true
    password?: true
    refreshToken?: true
    username?: true
  }

  export type UsersCountAggregateInputType = {
    id?: true
    name?: true
    image?: true
    password?: true
    refreshToken?: true
    username?: true
    _all?: true
  }

  export type UsersAggregateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which users to aggregate.
     */
    where?: usersWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of users to fetch.
     */
    orderBy?: usersOrderByWithRelationInput | usersOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the start position
     */
    cursor?: usersWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` users from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` users.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Count returned users
    **/
    _count?: true | UsersCountAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to average
    **/
    _avg?: UsersAvgAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to sum
    **/
    _sum?: UsersSumAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the minimum value
    **/
    _min?: UsersMinAggregateInputType
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/aggregations Aggregation Docs}
     * 
     * Select which fields to find the maximum value
    **/
    _max?: UsersMaxAggregateInputType
  }

  export type GetUsersAggregateType<T extends UsersAggregateArgs> = {
        [P in keyof T & keyof AggregateUsers]: P extends '_count' | 'count'
      ? T[P] extends true
        ? number
        : GetScalarType<T[P], AggregateUsers[P]>
      : GetScalarType<T[P], AggregateUsers[P]>
  }




  export type usersGroupByArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    where?: usersWhereInput
    orderBy?: usersOrderByWithAggregationInput | usersOrderByWithAggregationInput[]
    by: UsersScalarFieldEnum[] | UsersScalarFieldEnum
    having?: usersScalarWhereWithAggregatesInput
    take?: number
    skip?: number
    _count?: UsersCountAggregateInputType | true
    _avg?: UsersAvgAggregateInputType
    _sum?: UsersSumAggregateInputType
    _min?: UsersMinAggregateInputType
    _max?: UsersMaxAggregateInputType
  }

  export type UsersGroupByOutputType = {
    id: number
    name: string | null
    image: string | null
    password: string
    refreshToken: string | null
    username: string
    _count: UsersCountAggregateOutputType | null
    _avg: UsersAvgAggregateOutputType | null
    _sum: UsersSumAggregateOutputType | null
    _min: UsersMinAggregateOutputType | null
    _max: UsersMaxAggregateOutputType | null
  }

  type GetUsersGroupByPayload<T extends usersGroupByArgs> = Prisma.PrismaPromise<
    Array<
      PickEnumerable<UsersGroupByOutputType, T['by']> &
        {
          [P in ((keyof T) & (keyof UsersGroupByOutputType))]: P extends '_count'
            ? T[P] extends boolean
              ? number
              : GetScalarType<T[P], UsersGroupByOutputType[P]>
            : GetScalarType<T[P], UsersGroupByOutputType[P]>
        }
      >
    >


  export type usersSelect<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = $Extensions.GetSelect<{
    id?: boolean
    name?: boolean
    image?: boolean
    password?: boolean
    refreshToken?: boolean
    username?: boolean
    assignees?: boolean | users$assigneesArgs<ExtArgs>
    tasks?: boolean | users$tasksArgs<ExtArgs>
    _count?: boolean | UsersCountOutputTypeDefaultArgs<ExtArgs>
  }, ExtArgs["result"]["users"]>

  export type usersSelectScalar = {
    id?: boolean
    name?: boolean
    image?: boolean
    password?: boolean
    refreshToken?: boolean
    username?: boolean
  }

  export type usersInclude<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    assignees?: boolean | users$assigneesArgs<ExtArgs>
    tasks?: boolean | users$tasksArgs<ExtArgs>
    _count?: boolean | UsersCountOutputTypeDefaultArgs<ExtArgs>
  }


  export type $usersPayload<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    name: "users"
    objects: {
      assignees: Prisma.$assigneesPayload<ExtArgs>[]
      tasks: Prisma.$tasksPayload<ExtArgs>[]
    }
    scalars: $Extensions.GetResult<{
      id: number
      name: string | null
      image: string | null
      password: string
      refreshToken: string | null
      username: string
    }, ExtArgs["result"]["users"]>
    composites: {}
  }


  type usersGetPayload<S extends boolean | null | undefined | usersDefaultArgs> = $Result.GetResult<Prisma.$usersPayload, S>

  type usersCountArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = 
    Omit<usersFindManyArgs, 'select' | 'include'> & {
      select?: UsersCountAggregateInputType | true
    }

  export interface usersDelegate<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> {
    [K: symbol]: { types: Prisma.TypeMap<ExtArgs>['model']['users'], meta: { name: 'users' } }
    /**
     * Find zero or one Users that matches the filter.
     * @param {usersFindUniqueArgs} args - Arguments to find a Users
     * @example
     * // Get one Users
     * const users = await prisma.users.findUnique({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUnique<T extends usersFindUniqueArgs<ExtArgs>>(
      args: SelectSubset<T, usersFindUniqueArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findUnique'> | null, null, ExtArgs>

    /**
     * Find one Users that matches the filter or throw an error  with `error.code='P2025'` 
     *     if no matches were found.
     * @param {usersFindUniqueOrThrowArgs} args - Arguments to find a Users
     * @example
     * // Get one Users
     * const users = await prisma.users.findUniqueOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findUniqueOrThrow<T extends usersFindUniqueOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, usersFindUniqueOrThrowArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findUniqueOrThrow'>, never, ExtArgs>

    /**
     * Find the first Users that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersFindFirstArgs} args - Arguments to find a Users
     * @example
     * // Get one Users
     * const users = await prisma.users.findFirst({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirst<T extends usersFindFirstArgs<ExtArgs>>(
      args?: SelectSubset<T, usersFindFirstArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findFirst'> | null, null, ExtArgs>

    /**
     * Find the first Users that matches the filter or
     * throw `PrismaKnownClientError` with `P2025` code if no matches were found.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersFindFirstOrThrowArgs} args - Arguments to find a Users
     * @example
     * // Get one Users
     * const users = await prisma.users.findFirstOrThrow({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
    **/
    findFirstOrThrow<T extends usersFindFirstOrThrowArgs<ExtArgs>>(
      args?: SelectSubset<T, usersFindFirstOrThrowArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findFirstOrThrow'>, never, ExtArgs>

    /**
     * Find zero or more Users that matches the filter.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersFindManyArgs=} args - Arguments to filter and select certain fields only.
     * @example
     * // Get all Users
     * const users = await prisma.users.findMany()
     * 
     * // Get first 10 Users
     * const users = await prisma.users.findMany({ take: 10 })
     * 
     * // Only select the `id`
     * const usersWithIdOnly = await prisma.users.findMany({ select: { id: true } })
     * 
    **/
    findMany<T extends usersFindManyArgs<ExtArgs>>(
      args?: SelectSubset<T, usersFindManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'findMany'>>

    /**
     * Create a Users.
     * @param {usersCreateArgs} args - Arguments to create a Users.
     * @example
     * // Create one Users
     * const Users = await prisma.users.create({
     *   data: {
     *     // ... data to create a Users
     *   }
     * })
     * 
    **/
    create<T extends usersCreateArgs<ExtArgs>>(
      args: SelectSubset<T, usersCreateArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'create'>, never, ExtArgs>

    /**
     * Create many Users.
     *     @param {usersCreateManyArgs} args - Arguments to create many Users.
     *     @example
     *     // Create many Users
     *     const users = await prisma.users.createMany({
     *       data: {
     *         // ... provide data here
     *       }
     *     })
     *     
    **/
    createMany<T extends usersCreateManyArgs<ExtArgs>>(
      args?: SelectSubset<T, usersCreateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Delete a Users.
     * @param {usersDeleteArgs} args - Arguments to delete one Users.
     * @example
     * // Delete one Users
     * const Users = await prisma.users.delete({
     *   where: {
     *     // ... filter to delete one Users
     *   }
     * })
     * 
    **/
    delete<T extends usersDeleteArgs<ExtArgs>>(
      args: SelectSubset<T, usersDeleteArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'delete'>, never, ExtArgs>

    /**
     * Update one Users.
     * @param {usersUpdateArgs} args - Arguments to update one Users.
     * @example
     * // Update one Users
     * const users = await prisma.users.update({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    update<T extends usersUpdateArgs<ExtArgs>>(
      args: SelectSubset<T, usersUpdateArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'update'>, never, ExtArgs>

    /**
     * Delete zero or more Users.
     * @param {usersDeleteManyArgs} args - Arguments to filter Users to delete.
     * @example
     * // Delete a few Users
     * const { count } = await prisma.users.deleteMany({
     *   where: {
     *     // ... provide filter here
     *   }
     * })
     * 
    **/
    deleteMany<T extends usersDeleteManyArgs<ExtArgs>>(
      args?: SelectSubset<T, usersDeleteManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Update zero or more Users.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersUpdateManyArgs} args - Arguments to update one or more rows.
     * @example
     * // Update many Users
     * const users = await prisma.users.updateMany({
     *   where: {
     *     // ... provide filter here
     *   },
     *   data: {
     *     // ... provide data here
     *   }
     * })
     * 
    **/
    updateMany<T extends usersUpdateManyArgs<ExtArgs>>(
      args: SelectSubset<T, usersUpdateManyArgs<ExtArgs>>
    ): Prisma.PrismaPromise<BatchPayload>

    /**
     * Create or update one Users.
     * @param {usersUpsertArgs} args - Arguments to update or create a Users.
     * @example
     * // Update or create a Users
     * const users = await prisma.users.upsert({
     *   create: {
     *     // ... data to create a Users
     *   },
     *   update: {
     *     // ... in case it already exists, update
     *   },
     *   where: {
     *     // ... the filter for the Users we want to update
     *   }
     * })
    **/
    upsert<T extends usersUpsertArgs<ExtArgs>>(
      args: SelectSubset<T, usersUpsertArgs<ExtArgs>>
    ): Prisma__usersClient<$Result.GetResult<Prisma.$usersPayload<ExtArgs>, T, 'upsert'>, never, ExtArgs>

    /**
     * Count the number of Users.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersCountArgs} args - Arguments to filter Users to count.
     * @example
     * // Count the number of Users
     * const count = await prisma.users.count({
     *   where: {
     *     // ... the filter for the Users we want to count
     *   }
     * })
    **/
    count<T extends usersCountArgs>(
      args?: Subset<T, usersCountArgs>,
    ): Prisma.PrismaPromise<
      T extends $Utils.Record<'select', any>
        ? T['select'] extends true
          ? number
          : GetScalarType<T['select'], UsersCountAggregateOutputType>
        : number
    >

    /**
     * Allows you to perform aggregations operations on a Users.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {UsersAggregateArgs} args - Select which aggregations you would like to apply and on what fields.
     * @example
     * // Ordered by age ascending
     * // Where email contains prisma.io
     * // Limited to the 10 users
     * const aggregations = await prisma.user.aggregate({
     *   _avg: {
     *     age: true,
     *   },
     *   where: {
     *     email: {
     *       contains: "prisma.io",
     *     },
     *   },
     *   orderBy: {
     *     age: "asc",
     *   },
     *   take: 10,
     * })
    **/
    aggregate<T extends UsersAggregateArgs>(args: Subset<T, UsersAggregateArgs>): Prisma.PrismaPromise<GetUsersAggregateType<T>>

    /**
     * Group by Users.
     * Note, that providing `undefined` is treated as the value not being there.
     * Read more here: https://pris.ly/d/null-undefined
     * @param {usersGroupByArgs} args - Group by arguments.
     * @example
     * // Group by city, order by createdAt, get count
     * const result = await prisma.user.groupBy({
     *   by: ['city', 'createdAt'],
     *   orderBy: {
     *     createdAt: true
     *   },
     *   _count: {
     *     _all: true
     *   },
     * })
     * 
    **/
    groupBy<
      T extends usersGroupByArgs,
      HasSelectOrTake extends Or<
        Extends<'skip', Keys<T>>,
        Extends<'take', Keys<T>>
      >,
      OrderByArg extends True extends HasSelectOrTake
        ? { orderBy: usersGroupByArgs['orderBy'] }
        : { orderBy?: usersGroupByArgs['orderBy'] },
      OrderFields extends ExcludeUnderscoreKeys<Keys<MaybeTupleToUnion<T['orderBy']>>>,
      ByFields extends MaybeTupleToUnion<T['by']>,
      ByValid extends Has<ByFields, OrderFields>,
      HavingFields extends GetHavingFields<T['having']>,
      HavingValid extends Has<ByFields, HavingFields>,
      ByEmpty extends T['by'] extends never[] ? True : False,
      InputErrors extends ByEmpty extends True
      ? `Error: "by" must not be empty.`
      : HavingValid extends False
      ? {
          [P in HavingFields]: P extends ByFields
            ? never
            : P extends string
            ? `Error: Field "${P}" used in "having" needs to be provided in "by".`
            : [
                Error,
                'Field ',
                P,
                ` in "having" needs to be provided in "by"`,
              ]
        }[HavingFields]
      : 'take' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "take", you also need to provide "orderBy"'
      : 'skip' extends Keys<T>
      ? 'orderBy' extends Keys<T>
        ? ByValid extends True
          ? {}
          : {
              [P in OrderFields]: P extends ByFields
                ? never
                : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
            }[OrderFields]
        : 'Error: If you provide "skip", you also need to provide "orderBy"'
      : ByValid extends True
      ? {}
      : {
          [P in OrderFields]: P extends ByFields
            ? never
            : `Error: Field "${P}" in "orderBy" needs to be provided in "by"`
        }[OrderFields]
    >(args: SubsetIntersection<T, usersGroupByArgs, OrderByArg> & InputErrors): {} extends InputErrors ? GetUsersGroupByPayload<T> : Prisma.PrismaPromise<InputErrors>
  /**
   * Fields of the users model
   */
  readonly fields: usersFieldRefs;
  }

  /**
   * The delegate class that acts as a "Promise-like" for users.
   * Why is this prefixed with `Prisma__`?
   * Because we want to prevent naming conflicts as mentioned in
   * https://github.com/prisma/prisma-client-js/issues/707
   */
  export interface Prisma__usersClient<T, Null = never, ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> extends Prisma.PrismaPromise<T> {
    readonly [Symbol.toStringTag]: 'PrismaPromise';

    assignees<T extends users$assigneesArgs<ExtArgs> = {}>(args?: Subset<T, users$assigneesArgs<ExtArgs>>): Prisma.PrismaPromise<$Result.GetResult<Prisma.$assigneesPayload<ExtArgs>, T, 'findMany'> | Null>;

    tasks<T extends users$tasksArgs<ExtArgs> = {}>(args?: Subset<T, users$tasksArgs<ExtArgs>>): Prisma.PrismaPromise<$Result.GetResult<Prisma.$tasksPayload<ExtArgs>, T, 'findMany'> | Null>;

    /**
     * Attaches callbacks for the resolution and/or rejection of the Promise.
     * @param onfulfilled The callback to execute when the Promise is resolved.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of which ever callback is executed.
     */
    then<TResult1 = T, TResult2 = never>(onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | undefined | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null): $Utils.JsPromise<TResult1 | TResult2>;
    /**
     * Attaches a callback for only the rejection of the Promise.
     * @param onrejected The callback to execute when the Promise is rejected.
     * @returns A Promise for the completion of the callback.
     */
    catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null): $Utils.JsPromise<T | TResult>;
    /**
     * Attaches a callback that is invoked when the Promise is settled (fulfilled or rejected). The
     * resolved value cannot be modified from the callback.
     * @param onfinally The callback to execute when the Promise is settled (fulfilled or rejected).
     * @returns A Promise for the completion of the callback.
     */
    finally(onfinally?: (() => void) | undefined | null): $Utils.JsPromise<T>;
  }



  /**
   * Fields of the users model
   */ 
  interface usersFieldRefs {
    readonly id: FieldRef<"users", 'Int'>
    readonly name: FieldRef<"users", 'String'>
    readonly image: FieldRef<"users", 'String'>
    readonly password: FieldRef<"users", 'String'>
    readonly refreshToken: FieldRef<"users", 'String'>
    readonly username: FieldRef<"users", 'String'>
  }
    

  // Custom InputTypes

  /**
   * users findUnique
   */
  export type usersFindUniqueArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter, which users to fetch.
     */
    where: usersWhereUniqueInput
  }


  /**
   * users findUniqueOrThrow
   */
  export type usersFindUniqueOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter, which users to fetch.
     */
    where: usersWhereUniqueInput
  }


  /**
   * users findFirst
   */
  export type usersFindFirstArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter, which users to fetch.
     */
    where?: usersWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of users to fetch.
     */
    orderBy?: usersOrderByWithRelationInput | usersOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for users.
     */
    cursor?: usersWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` users from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` users.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of users.
     */
    distinct?: UsersScalarFieldEnum | UsersScalarFieldEnum[]
  }


  /**
   * users findFirstOrThrow
   */
  export type usersFindFirstOrThrowArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter, which users to fetch.
     */
    where?: usersWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of users to fetch.
     */
    orderBy?: usersOrderByWithRelationInput | usersOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for searching for users.
     */
    cursor?: usersWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` users from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` users.
     */
    skip?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/distinct Distinct Docs}
     * 
     * Filter by unique combinations of users.
     */
    distinct?: UsersScalarFieldEnum | UsersScalarFieldEnum[]
  }


  /**
   * users findMany
   */
  export type usersFindManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter, which users to fetch.
     */
    where?: usersWhereInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/sorting Sorting Docs}
     * 
     * Determine the order of users to fetch.
     */
    orderBy?: usersOrderByWithRelationInput | usersOrderByWithRelationInput[]
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination#cursor-based-pagination Cursor Docs}
     * 
     * Sets the position for listing users.
     */
    cursor?: usersWhereUniqueInput
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Take `±n` users from the position of the cursor.
     */
    take?: number
    /**
     * {@link https://www.prisma.io/docs/concepts/components/prisma-client/pagination Pagination Docs}
     * 
     * Skip the first `n` users.
     */
    skip?: number
    distinct?: UsersScalarFieldEnum | UsersScalarFieldEnum[]
  }


  /**
   * users create
   */
  export type usersCreateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * The data needed to create a users.
     */
    data: XOR<usersCreateInput, usersUncheckedCreateInput>
  }


  /**
   * users createMany
   */
  export type usersCreateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to create many users.
     */
    data: usersCreateManyInput | usersCreateManyInput[]
    skipDuplicates?: boolean
  }


  /**
   * users update
   */
  export type usersUpdateArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * The data needed to update a users.
     */
    data: XOR<usersUpdateInput, usersUncheckedUpdateInput>
    /**
     * Choose, which users to update.
     */
    where: usersWhereUniqueInput
  }


  /**
   * users updateMany
   */
  export type usersUpdateManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * The data used to update users.
     */
    data: XOR<usersUpdateManyMutationInput, usersUncheckedUpdateManyInput>
    /**
     * Filter which users to update
     */
    where?: usersWhereInput
  }


  /**
   * users upsert
   */
  export type usersUpsertArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * The filter to search for the users to update in case it exists.
     */
    where: usersWhereUniqueInput
    /**
     * In case the users found by the `where` argument doesn't exist, create a new users with this data.
     */
    create: XOR<usersCreateInput, usersUncheckedCreateInput>
    /**
     * In case the users was found with the provided `where` argument, update it with this data.
     */
    update: XOR<usersUpdateInput, usersUncheckedUpdateInput>
  }


  /**
   * users delete
   */
  export type usersDeleteArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
    /**
     * Filter which users to delete.
     */
    where: usersWhereUniqueInput
  }


  /**
   * users deleteMany
   */
  export type usersDeleteManyArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Filter which users to delete
     */
    where?: usersWhereInput
  }


  /**
   * users.assignees
   */
  export type users$assigneesArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the assignees
     */
    select?: assigneesSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: assigneesInclude<ExtArgs> | null
    where?: assigneesWhereInput
    orderBy?: assigneesOrderByWithRelationInput | assigneesOrderByWithRelationInput[]
    cursor?: assigneesWhereUniqueInput
    take?: number
    skip?: number
    distinct?: AssigneesScalarFieldEnum | AssigneesScalarFieldEnum[]
  }


  /**
   * users.tasks
   */
  export type users$tasksArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the tasks
     */
    select?: tasksSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: tasksInclude<ExtArgs> | null
    where?: tasksWhereInput
    orderBy?: tasksOrderByWithRelationInput | tasksOrderByWithRelationInput[]
    cursor?: tasksWhereUniqueInput
    take?: number
    skip?: number
    distinct?: TasksScalarFieldEnum | TasksScalarFieldEnum[]
  }


  /**
   * users without action
   */
  export type usersDefaultArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = {
    /**
     * Select specific fields to fetch from the users
     */
    select?: usersSelect<ExtArgs> | null
    /**
     * Choose, which related nodes to fetch as well.
     */
    include?: usersInclude<ExtArgs> | null
  }



  /**
   * Enums
   */

  export const TransactionIsolationLevel: {
    ReadUncommitted: 'ReadUncommitted',
    ReadCommitted: 'ReadCommitted',
    RepeatableRead: 'RepeatableRead',
    Serializable: 'Serializable'
  };

  export type TransactionIsolationLevel = (typeof TransactionIsolationLevel)[keyof typeof TransactionIsolationLevel]


  export const AssigneesScalarFieldEnum: {
    userId: 'userId',
    taskId: 'taskId'
  };

  export type AssigneesScalarFieldEnum = (typeof AssigneesScalarFieldEnum)[keyof typeof AssigneesScalarFieldEnum]


  export const TasksScalarFieldEnum: {
    id: 'id',
    title: 'title',
    created_at: 'created_at',
    due_at: 'due_at',
    creatorid: 'creatorid',
    isDone: 'isDone'
  };

  export type TasksScalarFieldEnum = (typeof TasksScalarFieldEnum)[keyof typeof TasksScalarFieldEnum]


  export const UsersScalarFieldEnum: {
    id: 'id',
    name: 'name',
    image: 'image',
    password: 'password',
    refreshToken: 'refreshToken',
    username: 'username'
  };

  export type UsersScalarFieldEnum = (typeof UsersScalarFieldEnum)[keyof typeof UsersScalarFieldEnum]


  export const SortOrder: {
    asc: 'asc',
    desc: 'desc'
  };

  export type SortOrder = (typeof SortOrder)[keyof typeof SortOrder]


  export const QueryMode: {
    default: 'default',
    insensitive: 'insensitive'
  };

  export type QueryMode = (typeof QueryMode)[keyof typeof QueryMode]


  export const NullsOrder: {
    first: 'first',
    last: 'last'
  };

  export type NullsOrder = (typeof NullsOrder)[keyof typeof NullsOrder]


  /**
   * Field references 
   */


  /**
   * Reference to a field of type 'Int'
   */
  export type IntFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'Int'>
    


  /**
   * Reference to a field of type 'Int[]'
   */
  export type ListIntFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'Int[]'>
    


  /**
   * Reference to a field of type 'String'
   */
  export type StringFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'String'>
    


  /**
   * Reference to a field of type 'String[]'
   */
  export type ListStringFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'String[]'>
    


  /**
   * Reference to a field of type 'DateTime'
   */
  export type DateTimeFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'DateTime'>
    


  /**
   * Reference to a field of type 'DateTime[]'
   */
  export type ListDateTimeFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'DateTime[]'>
    


  /**
   * Reference to a field of type 'Boolean'
   */
  export type BooleanFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'Boolean'>
    


  /**
   * Reference to a field of type 'Float'
   */
  export type FloatFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'Float'>
    


  /**
   * Reference to a field of type 'Float[]'
   */
  export type ListFloatFieldRefInput<$PrismaModel> = FieldRefInputType<$PrismaModel, 'Float[]'>
    
  /**
   * Deep Input Types
   */


  export type assigneesWhereInput = {
    AND?: assigneesWhereInput | assigneesWhereInput[]
    OR?: assigneesWhereInput[]
    NOT?: assigneesWhereInput | assigneesWhereInput[]
    userId?: IntFilter<"assignees"> | number
    taskId?: IntFilter<"assignees"> | number
    tasks?: XOR<TasksRelationFilter, tasksWhereInput>
    users?: XOR<UsersRelationFilter, usersWhereInput>
  }

  export type assigneesOrderByWithRelationInput = {
    userId?: SortOrder
    taskId?: SortOrder
    tasks?: tasksOrderByWithRelationInput
    users?: usersOrderByWithRelationInput
  }

  export type assigneesWhereUniqueInput = Prisma.AtLeast<{
    userId_taskId?: assigneesUserIdTaskIdCompoundUniqueInput
    AND?: assigneesWhereInput | assigneesWhereInput[]
    OR?: assigneesWhereInput[]
    NOT?: assigneesWhereInput | assigneesWhereInput[]
    userId?: IntFilter<"assignees"> | number
    taskId?: IntFilter<"assignees"> | number
    tasks?: XOR<TasksRelationFilter, tasksWhereInput>
    users?: XOR<UsersRelationFilter, usersWhereInput>
  }, "userId_taskId">

  export type assigneesOrderByWithAggregationInput = {
    userId?: SortOrder
    taskId?: SortOrder
    _count?: assigneesCountOrderByAggregateInput
    _avg?: assigneesAvgOrderByAggregateInput
    _max?: assigneesMaxOrderByAggregateInput
    _min?: assigneesMinOrderByAggregateInput
    _sum?: assigneesSumOrderByAggregateInput
  }

  export type assigneesScalarWhereWithAggregatesInput = {
    AND?: assigneesScalarWhereWithAggregatesInput | assigneesScalarWhereWithAggregatesInput[]
    OR?: assigneesScalarWhereWithAggregatesInput[]
    NOT?: assigneesScalarWhereWithAggregatesInput | assigneesScalarWhereWithAggregatesInput[]
    userId?: IntWithAggregatesFilter<"assignees"> | number
    taskId?: IntWithAggregatesFilter<"assignees"> | number
  }

  export type tasksWhereInput = {
    AND?: tasksWhereInput | tasksWhereInput[]
    OR?: tasksWhereInput[]
    NOT?: tasksWhereInput | tasksWhereInput[]
    id?: IntFilter<"tasks"> | number
    title?: StringNullableFilter<"tasks"> | string | null
    created_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    due_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    creatorid?: IntFilter<"tasks"> | number
    isDone?: BoolFilter<"tasks"> | boolean
    assignees?: AssigneesListRelationFilter
    creator?: XOR<UsersRelationFilter, usersWhereInput>
  }

  export type tasksOrderByWithRelationInput = {
    id?: SortOrder
    title?: SortOrderInput | SortOrder
    created_at?: SortOrderInput | SortOrder
    due_at?: SortOrderInput | SortOrder
    creatorid?: SortOrder
    isDone?: SortOrder
    assignees?: assigneesOrderByRelationAggregateInput
    creator?: usersOrderByWithRelationInput
  }

  export type tasksWhereUniqueInput = Prisma.AtLeast<{
    id?: number
    AND?: tasksWhereInput | tasksWhereInput[]
    OR?: tasksWhereInput[]
    NOT?: tasksWhereInput | tasksWhereInput[]
    title?: StringNullableFilter<"tasks"> | string | null
    created_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    due_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    creatorid?: IntFilter<"tasks"> | number
    isDone?: BoolFilter<"tasks"> | boolean
    assignees?: AssigneesListRelationFilter
    creator?: XOR<UsersRelationFilter, usersWhereInput>
  }, "id">

  export type tasksOrderByWithAggregationInput = {
    id?: SortOrder
    title?: SortOrderInput | SortOrder
    created_at?: SortOrderInput | SortOrder
    due_at?: SortOrderInput | SortOrder
    creatorid?: SortOrder
    isDone?: SortOrder
    _count?: tasksCountOrderByAggregateInput
    _avg?: tasksAvgOrderByAggregateInput
    _max?: tasksMaxOrderByAggregateInput
    _min?: tasksMinOrderByAggregateInput
    _sum?: tasksSumOrderByAggregateInput
  }

  export type tasksScalarWhereWithAggregatesInput = {
    AND?: tasksScalarWhereWithAggregatesInput | tasksScalarWhereWithAggregatesInput[]
    OR?: tasksScalarWhereWithAggregatesInput[]
    NOT?: tasksScalarWhereWithAggregatesInput | tasksScalarWhereWithAggregatesInput[]
    id?: IntWithAggregatesFilter<"tasks"> | number
    title?: StringNullableWithAggregatesFilter<"tasks"> | string | null
    created_at?: DateTimeNullableWithAggregatesFilter<"tasks"> | Date | string | null
    due_at?: DateTimeNullableWithAggregatesFilter<"tasks"> | Date | string | null
    creatorid?: IntWithAggregatesFilter<"tasks"> | number
    isDone?: BoolWithAggregatesFilter<"tasks"> | boolean
  }

  export type usersWhereInput = {
    AND?: usersWhereInput | usersWhereInput[]
    OR?: usersWhereInput[]
    NOT?: usersWhereInput | usersWhereInput[]
    id?: IntFilter<"users"> | number
    name?: StringNullableFilter<"users"> | string | null
    image?: StringNullableFilter<"users"> | string | null
    password?: StringFilter<"users"> | string
    refreshToken?: StringNullableFilter<"users"> | string | null
    username?: StringFilter<"users"> | string
    assignees?: AssigneesListRelationFilter
    tasks?: TasksListRelationFilter
  }

  export type usersOrderByWithRelationInput = {
    id?: SortOrder
    name?: SortOrderInput | SortOrder
    image?: SortOrderInput | SortOrder
    password?: SortOrder
    refreshToken?: SortOrderInput | SortOrder
    username?: SortOrder
    assignees?: assigneesOrderByRelationAggregateInput
    tasks?: tasksOrderByRelationAggregateInput
  }

  export type usersWhereUniqueInput = Prisma.AtLeast<{
    id?: number
    username?: string
    AND?: usersWhereInput | usersWhereInput[]
    OR?: usersWhereInput[]
    NOT?: usersWhereInput | usersWhereInput[]
    name?: StringNullableFilter<"users"> | string | null
    image?: StringNullableFilter<"users"> | string | null
    password?: StringFilter<"users"> | string
    refreshToken?: StringNullableFilter<"users"> | string | null
    assignees?: AssigneesListRelationFilter
    tasks?: TasksListRelationFilter
  }, "id" | "username">

  export type usersOrderByWithAggregationInput = {
    id?: SortOrder
    name?: SortOrderInput | SortOrder
    image?: SortOrderInput | SortOrder
    password?: SortOrder
    refreshToken?: SortOrderInput | SortOrder
    username?: SortOrder
    _count?: usersCountOrderByAggregateInput
    _avg?: usersAvgOrderByAggregateInput
    _max?: usersMaxOrderByAggregateInput
    _min?: usersMinOrderByAggregateInput
    _sum?: usersSumOrderByAggregateInput
  }

  export type usersScalarWhereWithAggregatesInput = {
    AND?: usersScalarWhereWithAggregatesInput | usersScalarWhereWithAggregatesInput[]
    OR?: usersScalarWhereWithAggregatesInput[]
    NOT?: usersScalarWhereWithAggregatesInput | usersScalarWhereWithAggregatesInput[]
    id?: IntWithAggregatesFilter<"users"> | number
    name?: StringNullableWithAggregatesFilter<"users"> | string | null
    image?: StringNullableWithAggregatesFilter<"users"> | string | null
    password?: StringWithAggregatesFilter<"users"> | string
    refreshToken?: StringNullableWithAggregatesFilter<"users"> | string | null
    username?: StringWithAggregatesFilter<"users"> | string
  }

  export type assigneesCreateInput = {
    tasks: tasksCreateNestedOneWithoutAssigneesInput
    users: usersCreateNestedOneWithoutAssigneesInput
  }

  export type assigneesUncheckedCreateInput = {
    userId: number
    taskId: number
  }

  export type assigneesUpdateInput = {
    tasks?: tasksUpdateOneRequiredWithoutAssigneesNestedInput
    users?: usersUpdateOneRequiredWithoutAssigneesNestedInput
  }

  export type assigneesUncheckedUpdateInput = {
    userId?: IntFieldUpdateOperationsInput | number
    taskId?: IntFieldUpdateOperationsInput | number
  }

  export type assigneesCreateManyInput = {
    userId: number
    taskId: number
  }

  export type assigneesUpdateManyMutationInput = {

  }

  export type assigneesUncheckedUpdateManyInput = {
    userId?: IntFieldUpdateOperationsInput | number
    taskId?: IntFieldUpdateOperationsInput | number
  }

  export type tasksCreateInput = {
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    isDone?: boolean
    assignees?: assigneesCreateNestedManyWithoutTasksInput
    creator: usersCreateNestedOneWithoutTasksInput
  }

  export type tasksUncheckedCreateInput = {
    id?: number
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    creatorid: number
    isDone?: boolean
    assignees?: assigneesUncheckedCreateNestedManyWithoutTasksInput
  }

  export type tasksUpdateInput = {
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
    assignees?: assigneesUpdateManyWithoutTasksNestedInput
    creator?: usersUpdateOneRequiredWithoutTasksNestedInput
  }

  export type tasksUncheckedUpdateInput = {
    id?: IntFieldUpdateOperationsInput | number
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    creatorid?: IntFieldUpdateOperationsInput | number
    isDone?: BoolFieldUpdateOperationsInput | boolean
    assignees?: assigneesUncheckedUpdateManyWithoutTasksNestedInput
  }

  export type tasksCreateManyInput = {
    id?: number
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    creatorid: number
    isDone?: boolean
  }

  export type tasksUpdateManyMutationInput = {
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
  }

  export type tasksUncheckedUpdateManyInput = {
    id?: IntFieldUpdateOperationsInput | number
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    creatorid?: IntFieldUpdateOperationsInput | number
    isDone?: BoolFieldUpdateOperationsInput | boolean
  }

  export type usersCreateInput = {
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    assignees?: assigneesCreateNestedManyWithoutUsersInput
    tasks?: tasksCreateNestedManyWithoutCreatorInput
  }

  export type usersUncheckedCreateInput = {
    id?: number
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    assignees?: assigneesUncheckedCreateNestedManyWithoutUsersInput
    tasks?: tasksUncheckedCreateNestedManyWithoutCreatorInput
  }

  export type usersUpdateInput = {
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    assignees?: assigneesUpdateManyWithoutUsersNestedInput
    tasks?: tasksUpdateManyWithoutCreatorNestedInput
  }

  export type usersUncheckedUpdateInput = {
    id?: IntFieldUpdateOperationsInput | number
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    assignees?: assigneesUncheckedUpdateManyWithoutUsersNestedInput
    tasks?: tasksUncheckedUpdateManyWithoutCreatorNestedInput
  }

  export type usersCreateManyInput = {
    id?: number
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
  }

  export type usersUpdateManyMutationInput = {
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
  }

  export type usersUncheckedUpdateManyInput = {
    id?: IntFieldUpdateOperationsInput | number
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
  }

  export type IntFilter<$PrismaModel = never> = {
    equals?: number | IntFieldRefInput<$PrismaModel>
    in?: number[] | ListIntFieldRefInput<$PrismaModel>
    notIn?: number[] | ListIntFieldRefInput<$PrismaModel>
    lt?: number | IntFieldRefInput<$PrismaModel>
    lte?: number | IntFieldRefInput<$PrismaModel>
    gt?: number | IntFieldRefInput<$PrismaModel>
    gte?: number | IntFieldRefInput<$PrismaModel>
    not?: NestedIntFilter<$PrismaModel> | number
  }

  export type TasksRelationFilter = {
    is?: tasksWhereInput
    isNot?: tasksWhereInput
  }

  export type UsersRelationFilter = {
    is?: usersWhereInput
    isNot?: usersWhereInput
  }

  export type assigneesUserIdTaskIdCompoundUniqueInput = {
    userId: number
    taskId: number
  }

  export type assigneesCountOrderByAggregateInput = {
    userId?: SortOrder
    taskId?: SortOrder
  }

  export type assigneesAvgOrderByAggregateInput = {
    userId?: SortOrder
    taskId?: SortOrder
  }

  export type assigneesMaxOrderByAggregateInput = {
    userId?: SortOrder
    taskId?: SortOrder
  }

  export type assigneesMinOrderByAggregateInput = {
    userId?: SortOrder
    taskId?: SortOrder
  }

  export type assigneesSumOrderByAggregateInput = {
    userId?: SortOrder
    taskId?: SortOrder
  }

  export type IntWithAggregatesFilter<$PrismaModel = never> = {
    equals?: number | IntFieldRefInput<$PrismaModel>
    in?: number[] | ListIntFieldRefInput<$PrismaModel>
    notIn?: number[] | ListIntFieldRefInput<$PrismaModel>
    lt?: number | IntFieldRefInput<$PrismaModel>
    lte?: number | IntFieldRefInput<$PrismaModel>
    gt?: number | IntFieldRefInput<$PrismaModel>
    gte?: number | IntFieldRefInput<$PrismaModel>
    not?: NestedIntWithAggregatesFilter<$PrismaModel> | number
    _count?: NestedIntFilter<$PrismaModel>
    _avg?: NestedFloatFilter<$PrismaModel>
    _sum?: NestedIntFilter<$PrismaModel>
    _min?: NestedIntFilter<$PrismaModel>
    _max?: NestedIntFilter<$PrismaModel>
  }

  export type StringNullableFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel> | null
    in?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    mode?: QueryMode
    not?: NestedStringNullableFilter<$PrismaModel> | string | null
  }

  export type DateTimeNullableFilter<$PrismaModel = never> = {
    equals?: Date | string | DateTimeFieldRefInput<$PrismaModel> | null
    in?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    notIn?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    lt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    lte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    not?: NestedDateTimeNullableFilter<$PrismaModel> | Date | string | null
  }

  export type BoolFilter<$PrismaModel = never> = {
    equals?: boolean | BooleanFieldRefInput<$PrismaModel>
    not?: NestedBoolFilter<$PrismaModel> | boolean
  }

  export type AssigneesListRelationFilter = {
    every?: assigneesWhereInput
    some?: assigneesWhereInput
    none?: assigneesWhereInput
  }

  export type SortOrderInput = {
    sort: SortOrder
    nulls?: NullsOrder
  }

  export type assigneesOrderByRelationAggregateInput = {
    _count?: SortOrder
  }

  export type tasksCountOrderByAggregateInput = {
    id?: SortOrder
    title?: SortOrder
    created_at?: SortOrder
    due_at?: SortOrder
    creatorid?: SortOrder
    isDone?: SortOrder
  }

  export type tasksAvgOrderByAggregateInput = {
    id?: SortOrder
    creatorid?: SortOrder
  }

  export type tasksMaxOrderByAggregateInput = {
    id?: SortOrder
    title?: SortOrder
    created_at?: SortOrder
    due_at?: SortOrder
    creatorid?: SortOrder
    isDone?: SortOrder
  }

  export type tasksMinOrderByAggregateInput = {
    id?: SortOrder
    title?: SortOrder
    created_at?: SortOrder
    due_at?: SortOrder
    creatorid?: SortOrder
    isDone?: SortOrder
  }

  export type tasksSumOrderByAggregateInput = {
    id?: SortOrder
    creatorid?: SortOrder
  }

  export type StringNullableWithAggregatesFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel> | null
    in?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    mode?: QueryMode
    not?: NestedStringNullableWithAggregatesFilter<$PrismaModel> | string | null
    _count?: NestedIntNullableFilter<$PrismaModel>
    _min?: NestedStringNullableFilter<$PrismaModel>
    _max?: NestedStringNullableFilter<$PrismaModel>
  }

  export type DateTimeNullableWithAggregatesFilter<$PrismaModel = never> = {
    equals?: Date | string | DateTimeFieldRefInput<$PrismaModel> | null
    in?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    notIn?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    lt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    lte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    not?: NestedDateTimeNullableWithAggregatesFilter<$PrismaModel> | Date | string | null
    _count?: NestedIntNullableFilter<$PrismaModel>
    _min?: NestedDateTimeNullableFilter<$PrismaModel>
    _max?: NestedDateTimeNullableFilter<$PrismaModel>
  }

  export type BoolWithAggregatesFilter<$PrismaModel = never> = {
    equals?: boolean | BooleanFieldRefInput<$PrismaModel>
    not?: NestedBoolWithAggregatesFilter<$PrismaModel> | boolean
    _count?: NestedIntFilter<$PrismaModel>
    _min?: NestedBoolFilter<$PrismaModel>
    _max?: NestedBoolFilter<$PrismaModel>
  }

  export type StringFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel>
    in?: string[] | ListStringFieldRefInput<$PrismaModel>
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel>
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    mode?: QueryMode
    not?: NestedStringFilter<$PrismaModel> | string
  }

  export type TasksListRelationFilter = {
    every?: tasksWhereInput
    some?: tasksWhereInput
    none?: tasksWhereInput
  }

  export type tasksOrderByRelationAggregateInput = {
    _count?: SortOrder
  }

  export type usersCountOrderByAggregateInput = {
    id?: SortOrder
    name?: SortOrder
    image?: SortOrder
    password?: SortOrder
    refreshToken?: SortOrder
    username?: SortOrder
  }

  export type usersAvgOrderByAggregateInput = {
    id?: SortOrder
  }

  export type usersMaxOrderByAggregateInput = {
    id?: SortOrder
    name?: SortOrder
    image?: SortOrder
    password?: SortOrder
    refreshToken?: SortOrder
    username?: SortOrder
  }

  export type usersMinOrderByAggregateInput = {
    id?: SortOrder
    name?: SortOrder
    image?: SortOrder
    password?: SortOrder
    refreshToken?: SortOrder
    username?: SortOrder
  }

  export type usersSumOrderByAggregateInput = {
    id?: SortOrder
  }

  export type StringWithAggregatesFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel>
    in?: string[] | ListStringFieldRefInput<$PrismaModel>
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel>
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    mode?: QueryMode
    not?: NestedStringWithAggregatesFilter<$PrismaModel> | string
    _count?: NestedIntFilter<$PrismaModel>
    _min?: NestedStringFilter<$PrismaModel>
    _max?: NestedStringFilter<$PrismaModel>
  }

  export type tasksCreateNestedOneWithoutAssigneesInput = {
    create?: XOR<tasksCreateWithoutAssigneesInput, tasksUncheckedCreateWithoutAssigneesInput>
    connectOrCreate?: tasksCreateOrConnectWithoutAssigneesInput
    connect?: tasksWhereUniqueInput
  }

  export type usersCreateNestedOneWithoutAssigneesInput = {
    create?: XOR<usersCreateWithoutAssigneesInput, usersUncheckedCreateWithoutAssigneesInput>
    connectOrCreate?: usersCreateOrConnectWithoutAssigneesInput
    connect?: usersWhereUniqueInput
  }

  export type tasksUpdateOneRequiredWithoutAssigneesNestedInput = {
    create?: XOR<tasksCreateWithoutAssigneesInput, tasksUncheckedCreateWithoutAssigneesInput>
    connectOrCreate?: tasksCreateOrConnectWithoutAssigneesInput
    upsert?: tasksUpsertWithoutAssigneesInput
    connect?: tasksWhereUniqueInput
    update?: XOR<XOR<tasksUpdateToOneWithWhereWithoutAssigneesInput, tasksUpdateWithoutAssigneesInput>, tasksUncheckedUpdateWithoutAssigneesInput>
  }

  export type usersUpdateOneRequiredWithoutAssigneesNestedInput = {
    create?: XOR<usersCreateWithoutAssigneesInput, usersUncheckedCreateWithoutAssigneesInput>
    connectOrCreate?: usersCreateOrConnectWithoutAssigneesInput
    upsert?: usersUpsertWithoutAssigneesInput
    connect?: usersWhereUniqueInput
    update?: XOR<XOR<usersUpdateToOneWithWhereWithoutAssigneesInput, usersUpdateWithoutAssigneesInput>, usersUncheckedUpdateWithoutAssigneesInput>
  }

  export type IntFieldUpdateOperationsInput = {
    set?: number
    increment?: number
    decrement?: number
    multiply?: number
    divide?: number
  }

  export type assigneesCreateNestedManyWithoutTasksInput = {
    create?: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput> | assigneesCreateWithoutTasksInput[] | assigneesUncheckedCreateWithoutTasksInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutTasksInput | assigneesCreateOrConnectWithoutTasksInput[]
    createMany?: assigneesCreateManyTasksInputEnvelope
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
  }

  export type usersCreateNestedOneWithoutTasksInput = {
    create?: XOR<usersCreateWithoutTasksInput, usersUncheckedCreateWithoutTasksInput>
    connectOrCreate?: usersCreateOrConnectWithoutTasksInput
    connect?: usersWhereUniqueInput
  }

  export type assigneesUncheckedCreateNestedManyWithoutTasksInput = {
    create?: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput> | assigneesCreateWithoutTasksInput[] | assigneesUncheckedCreateWithoutTasksInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutTasksInput | assigneesCreateOrConnectWithoutTasksInput[]
    createMany?: assigneesCreateManyTasksInputEnvelope
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
  }

  export type NullableStringFieldUpdateOperationsInput = {
    set?: string | null
  }

  export type NullableDateTimeFieldUpdateOperationsInput = {
    set?: Date | string | null
  }

  export type BoolFieldUpdateOperationsInput = {
    set?: boolean
  }

  export type assigneesUpdateManyWithoutTasksNestedInput = {
    create?: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput> | assigneesCreateWithoutTasksInput[] | assigneesUncheckedCreateWithoutTasksInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutTasksInput | assigneesCreateOrConnectWithoutTasksInput[]
    upsert?: assigneesUpsertWithWhereUniqueWithoutTasksInput | assigneesUpsertWithWhereUniqueWithoutTasksInput[]
    createMany?: assigneesCreateManyTasksInputEnvelope
    set?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    disconnect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    delete?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    update?: assigneesUpdateWithWhereUniqueWithoutTasksInput | assigneesUpdateWithWhereUniqueWithoutTasksInput[]
    updateMany?: assigneesUpdateManyWithWhereWithoutTasksInput | assigneesUpdateManyWithWhereWithoutTasksInput[]
    deleteMany?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
  }

  export type usersUpdateOneRequiredWithoutTasksNestedInput = {
    create?: XOR<usersCreateWithoutTasksInput, usersUncheckedCreateWithoutTasksInput>
    connectOrCreate?: usersCreateOrConnectWithoutTasksInput
    upsert?: usersUpsertWithoutTasksInput
    connect?: usersWhereUniqueInput
    update?: XOR<XOR<usersUpdateToOneWithWhereWithoutTasksInput, usersUpdateWithoutTasksInput>, usersUncheckedUpdateWithoutTasksInput>
  }

  export type assigneesUncheckedUpdateManyWithoutTasksNestedInput = {
    create?: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput> | assigneesCreateWithoutTasksInput[] | assigneesUncheckedCreateWithoutTasksInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutTasksInput | assigneesCreateOrConnectWithoutTasksInput[]
    upsert?: assigneesUpsertWithWhereUniqueWithoutTasksInput | assigneesUpsertWithWhereUniqueWithoutTasksInput[]
    createMany?: assigneesCreateManyTasksInputEnvelope
    set?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    disconnect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    delete?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    update?: assigneesUpdateWithWhereUniqueWithoutTasksInput | assigneesUpdateWithWhereUniqueWithoutTasksInput[]
    updateMany?: assigneesUpdateManyWithWhereWithoutTasksInput | assigneesUpdateManyWithWhereWithoutTasksInput[]
    deleteMany?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
  }

  export type assigneesCreateNestedManyWithoutUsersInput = {
    create?: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput> | assigneesCreateWithoutUsersInput[] | assigneesUncheckedCreateWithoutUsersInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutUsersInput | assigneesCreateOrConnectWithoutUsersInput[]
    createMany?: assigneesCreateManyUsersInputEnvelope
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
  }

  export type tasksCreateNestedManyWithoutCreatorInput = {
    create?: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput> | tasksCreateWithoutCreatorInput[] | tasksUncheckedCreateWithoutCreatorInput[]
    connectOrCreate?: tasksCreateOrConnectWithoutCreatorInput | tasksCreateOrConnectWithoutCreatorInput[]
    createMany?: tasksCreateManyCreatorInputEnvelope
    connect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
  }

  export type assigneesUncheckedCreateNestedManyWithoutUsersInput = {
    create?: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput> | assigneesCreateWithoutUsersInput[] | assigneesUncheckedCreateWithoutUsersInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutUsersInput | assigneesCreateOrConnectWithoutUsersInput[]
    createMany?: assigneesCreateManyUsersInputEnvelope
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
  }

  export type tasksUncheckedCreateNestedManyWithoutCreatorInput = {
    create?: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput> | tasksCreateWithoutCreatorInput[] | tasksUncheckedCreateWithoutCreatorInput[]
    connectOrCreate?: tasksCreateOrConnectWithoutCreatorInput | tasksCreateOrConnectWithoutCreatorInput[]
    createMany?: tasksCreateManyCreatorInputEnvelope
    connect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
  }

  export type StringFieldUpdateOperationsInput = {
    set?: string
  }

  export type assigneesUpdateManyWithoutUsersNestedInput = {
    create?: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput> | assigneesCreateWithoutUsersInput[] | assigneesUncheckedCreateWithoutUsersInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutUsersInput | assigneesCreateOrConnectWithoutUsersInput[]
    upsert?: assigneesUpsertWithWhereUniqueWithoutUsersInput | assigneesUpsertWithWhereUniqueWithoutUsersInput[]
    createMany?: assigneesCreateManyUsersInputEnvelope
    set?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    disconnect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    delete?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    update?: assigneesUpdateWithWhereUniqueWithoutUsersInput | assigneesUpdateWithWhereUniqueWithoutUsersInput[]
    updateMany?: assigneesUpdateManyWithWhereWithoutUsersInput | assigneesUpdateManyWithWhereWithoutUsersInput[]
    deleteMany?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
  }

  export type tasksUpdateManyWithoutCreatorNestedInput = {
    create?: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput> | tasksCreateWithoutCreatorInput[] | tasksUncheckedCreateWithoutCreatorInput[]
    connectOrCreate?: tasksCreateOrConnectWithoutCreatorInput | tasksCreateOrConnectWithoutCreatorInput[]
    upsert?: tasksUpsertWithWhereUniqueWithoutCreatorInput | tasksUpsertWithWhereUniqueWithoutCreatorInput[]
    createMany?: tasksCreateManyCreatorInputEnvelope
    set?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    disconnect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    delete?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    connect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    update?: tasksUpdateWithWhereUniqueWithoutCreatorInput | tasksUpdateWithWhereUniqueWithoutCreatorInput[]
    updateMany?: tasksUpdateManyWithWhereWithoutCreatorInput | tasksUpdateManyWithWhereWithoutCreatorInput[]
    deleteMany?: tasksScalarWhereInput | tasksScalarWhereInput[]
  }

  export type assigneesUncheckedUpdateManyWithoutUsersNestedInput = {
    create?: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput> | assigneesCreateWithoutUsersInput[] | assigneesUncheckedCreateWithoutUsersInput[]
    connectOrCreate?: assigneesCreateOrConnectWithoutUsersInput | assigneesCreateOrConnectWithoutUsersInput[]
    upsert?: assigneesUpsertWithWhereUniqueWithoutUsersInput | assigneesUpsertWithWhereUniqueWithoutUsersInput[]
    createMany?: assigneesCreateManyUsersInputEnvelope
    set?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    disconnect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    delete?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    connect?: assigneesWhereUniqueInput | assigneesWhereUniqueInput[]
    update?: assigneesUpdateWithWhereUniqueWithoutUsersInput | assigneesUpdateWithWhereUniqueWithoutUsersInput[]
    updateMany?: assigneesUpdateManyWithWhereWithoutUsersInput | assigneesUpdateManyWithWhereWithoutUsersInput[]
    deleteMany?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
  }

  export type tasksUncheckedUpdateManyWithoutCreatorNestedInput = {
    create?: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput> | tasksCreateWithoutCreatorInput[] | tasksUncheckedCreateWithoutCreatorInput[]
    connectOrCreate?: tasksCreateOrConnectWithoutCreatorInput | tasksCreateOrConnectWithoutCreatorInput[]
    upsert?: tasksUpsertWithWhereUniqueWithoutCreatorInput | tasksUpsertWithWhereUniqueWithoutCreatorInput[]
    createMany?: tasksCreateManyCreatorInputEnvelope
    set?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    disconnect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    delete?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    connect?: tasksWhereUniqueInput | tasksWhereUniqueInput[]
    update?: tasksUpdateWithWhereUniqueWithoutCreatorInput | tasksUpdateWithWhereUniqueWithoutCreatorInput[]
    updateMany?: tasksUpdateManyWithWhereWithoutCreatorInput | tasksUpdateManyWithWhereWithoutCreatorInput[]
    deleteMany?: tasksScalarWhereInput | tasksScalarWhereInput[]
  }

  export type NestedIntFilter<$PrismaModel = never> = {
    equals?: number | IntFieldRefInput<$PrismaModel>
    in?: number[] | ListIntFieldRefInput<$PrismaModel>
    notIn?: number[] | ListIntFieldRefInput<$PrismaModel>
    lt?: number | IntFieldRefInput<$PrismaModel>
    lte?: number | IntFieldRefInput<$PrismaModel>
    gt?: number | IntFieldRefInput<$PrismaModel>
    gte?: number | IntFieldRefInput<$PrismaModel>
    not?: NestedIntFilter<$PrismaModel> | number
  }

  export type NestedIntWithAggregatesFilter<$PrismaModel = never> = {
    equals?: number | IntFieldRefInput<$PrismaModel>
    in?: number[] | ListIntFieldRefInput<$PrismaModel>
    notIn?: number[] | ListIntFieldRefInput<$PrismaModel>
    lt?: number | IntFieldRefInput<$PrismaModel>
    lte?: number | IntFieldRefInput<$PrismaModel>
    gt?: number | IntFieldRefInput<$PrismaModel>
    gte?: number | IntFieldRefInput<$PrismaModel>
    not?: NestedIntWithAggregatesFilter<$PrismaModel> | number
    _count?: NestedIntFilter<$PrismaModel>
    _avg?: NestedFloatFilter<$PrismaModel>
    _sum?: NestedIntFilter<$PrismaModel>
    _min?: NestedIntFilter<$PrismaModel>
    _max?: NestedIntFilter<$PrismaModel>
  }

  export type NestedFloatFilter<$PrismaModel = never> = {
    equals?: number | FloatFieldRefInput<$PrismaModel>
    in?: number[] | ListFloatFieldRefInput<$PrismaModel>
    notIn?: number[] | ListFloatFieldRefInput<$PrismaModel>
    lt?: number | FloatFieldRefInput<$PrismaModel>
    lte?: number | FloatFieldRefInput<$PrismaModel>
    gt?: number | FloatFieldRefInput<$PrismaModel>
    gte?: number | FloatFieldRefInput<$PrismaModel>
    not?: NestedFloatFilter<$PrismaModel> | number
  }

  export type NestedStringNullableFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel> | null
    in?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    not?: NestedStringNullableFilter<$PrismaModel> | string | null
  }

  export type NestedDateTimeNullableFilter<$PrismaModel = never> = {
    equals?: Date | string | DateTimeFieldRefInput<$PrismaModel> | null
    in?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    notIn?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    lt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    lte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    not?: NestedDateTimeNullableFilter<$PrismaModel> | Date | string | null
  }

  export type NestedBoolFilter<$PrismaModel = never> = {
    equals?: boolean | BooleanFieldRefInput<$PrismaModel>
    not?: NestedBoolFilter<$PrismaModel> | boolean
  }

  export type NestedStringNullableWithAggregatesFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel> | null
    in?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel> | null
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    not?: NestedStringNullableWithAggregatesFilter<$PrismaModel> | string | null
    _count?: NestedIntNullableFilter<$PrismaModel>
    _min?: NestedStringNullableFilter<$PrismaModel>
    _max?: NestedStringNullableFilter<$PrismaModel>
  }

  export type NestedIntNullableFilter<$PrismaModel = never> = {
    equals?: number | IntFieldRefInput<$PrismaModel> | null
    in?: number[] | ListIntFieldRefInput<$PrismaModel> | null
    notIn?: number[] | ListIntFieldRefInput<$PrismaModel> | null
    lt?: number | IntFieldRefInput<$PrismaModel>
    lte?: number | IntFieldRefInput<$PrismaModel>
    gt?: number | IntFieldRefInput<$PrismaModel>
    gte?: number | IntFieldRefInput<$PrismaModel>
    not?: NestedIntNullableFilter<$PrismaModel> | number | null
  }

  export type NestedDateTimeNullableWithAggregatesFilter<$PrismaModel = never> = {
    equals?: Date | string | DateTimeFieldRefInput<$PrismaModel> | null
    in?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    notIn?: Date[] | string[] | ListDateTimeFieldRefInput<$PrismaModel> | null
    lt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    lte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gt?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    gte?: Date | string | DateTimeFieldRefInput<$PrismaModel>
    not?: NestedDateTimeNullableWithAggregatesFilter<$PrismaModel> | Date | string | null
    _count?: NestedIntNullableFilter<$PrismaModel>
    _min?: NestedDateTimeNullableFilter<$PrismaModel>
    _max?: NestedDateTimeNullableFilter<$PrismaModel>
  }

  export type NestedBoolWithAggregatesFilter<$PrismaModel = never> = {
    equals?: boolean | BooleanFieldRefInput<$PrismaModel>
    not?: NestedBoolWithAggregatesFilter<$PrismaModel> | boolean
    _count?: NestedIntFilter<$PrismaModel>
    _min?: NestedBoolFilter<$PrismaModel>
    _max?: NestedBoolFilter<$PrismaModel>
  }

  export type NestedStringFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel>
    in?: string[] | ListStringFieldRefInput<$PrismaModel>
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel>
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    not?: NestedStringFilter<$PrismaModel> | string
  }

  export type NestedStringWithAggregatesFilter<$PrismaModel = never> = {
    equals?: string | StringFieldRefInput<$PrismaModel>
    in?: string[] | ListStringFieldRefInput<$PrismaModel>
    notIn?: string[] | ListStringFieldRefInput<$PrismaModel>
    lt?: string | StringFieldRefInput<$PrismaModel>
    lte?: string | StringFieldRefInput<$PrismaModel>
    gt?: string | StringFieldRefInput<$PrismaModel>
    gte?: string | StringFieldRefInput<$PrismaModel>
    contains?: string | StringFieldRefInput<$PrismaModel>
    startsWith?: string | StringFieldRefInput<$PrismaModel>
    endsWith?: string | StringFieldRefInput<$PrismaModel>
    not?: NestedStringWithAggregatesFilter<$PrismaModel> | string
    _count?: NestedIntFilter<$PrismaModel>
    _min?: NestedStringFilter<$PrismaModel>
    _max?: NestedStringFilter<$PrismaModel>
  }

  export type tasksCreateWithoutAssigneesInput = {
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    isDone?: boolean
    creator: usersCreateNestedOneWithoutTasksInput
  }

  export type tasksUncheckedCreateWithoutAssigneesInput = {
    id?: number
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    creatorid: number
    isDone?: boolean
  }

  export type tasksCreateOrConnectWithoutAssigneesInput = {
    where: tasksWhereUniqueInput
    create: XOR<tasksCreateWithoutAssigneesInput, tasksUncheckedCreateWithoutAssigneesInput>
  }

  export type usersCreateWithoutAssigneesInput = {
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    tasks?: tasksCreateNestedManyWithoutCreatorInput
  }

  export type usersUncheckedCreateWithoutAssigneesInput = {
    id?: number
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    tasks?: tasksUncheckedCreateNestedManyWithoutCreatorInput
  }

  export type usersCreateOrConnectWithoutAssigneesInput = {
    where: usersWhereUniqueInput
    create: XOR<usersCreateWithoutAssigneesInput, usersUncheckedCreateWithoutAssigneesInput>
  }

  export type tasksUpsertWithoutAssigneesInput = {
    update: XOR<tasksUpdateWithoutAssigneesInput, tasksUncheckedUpdateWithoutAssigneesInput>
    create: XOR<tasksCreateWithoutAssigneesInput, tasksUncheckedCreateWithoutAssigneesInput>
    where?: tasksWhereInput
  }

  export type tasksUpdateToOneWithWhereWithoutAssigneesInput = {
    where?: tasksWhereInput
    data: XOR<tasksUpdateWithoutAssigneesInput, tasksUncheckedUpdateWithoutAssigneesInput>
  }

  export type tasksUpdateWithoutAssigneesInput = {
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
    creator?: usersUpdateOneRequiredWithoutTasksNestedInput
  }

  export type tasksUncheckedUpdateWithoutAssigneesInput = {
    id?: IntFieldUpdateOperationsInput | number
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    creatorid?: IntFieldUpdateOperationsInput | number
    isDone?: BoolFieldUpdateOperationsInput | boolean
  }

  export type usersUpsertWithoutAssigneesInput = {
    update: XOR<usersUpdateWithoutAssigneesInput, usersUncheckedUpdateWithoutAssigneesInput>
    create: XOR<usersCreateWithoutAssigneesInput, usersUncheckedCreateWithoutAssigneesInput>
    where?: usersWhereInput
  }

  export type usersUpdateToOneWithWhereWithoutAssigneesInput = {
    where?: usersWhereInput
    data: XOR<usersUpdateWithoutAssigneesInput, usersUncheckedUpdateWithoutAssigneesInput>
  }

  export type usersUpdateWithoutAssigneesInput = {
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    tasks?: tasksUpdateManyWithoutCreatorNestedInput
  }

  export type usersUncheckedUpdateWithoutAssigneesInput = {
    id?: IntFieldUpdateOperationsInput | number
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    tasks?: tasksUncheckedUpdateManyWithoutCreatorNestedInput
  }

  export type assigneesCreateWithoutTasksInput = {
    users: usersCreateNestedOneWithoutAssigneesInput
  }

  export type assigneesUncheckedCreateWithoutTasksInput = {
    userId: number
  }

  export type assigneesCreateOrConnectWithoutTasksInput = {
    where: assigneesWhereUniqueInput
    create: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput>
  }

  export type assigneesCreateManyTasksInputEnvelope = {
    data: assigneesCreateManyTasksInput | assigneesCreateManyTasksInput[]
    skipDuplicates?: boolean
  }

  export type usersCreateWithoutTasksInput = {
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    assignees?: assigneesCreateNestedManyWithoutUsersInput
  }

  export type usersUncheckedCreateWithoutTasksInput = {
    id?: number
    name?: string | null
    image?: string | null
    password: string
    refreshToken?: string | null
    username: string
    assignees?: assigneesUncheckedCreateNestedManyWithoutUsersInput
  }

  export type usersCreateOrConnectWithoutTasksInput = {
    where: usersWhereUniqueInput
    create: XOR<usersCreateWithoutTasksInput, usersUncheckedCreateWithoutTasksInput>
  }

  export type assigneesUpsertWithWhereUniqueWithoutTasksInput = {
    where: assigneesWhereUniqueInput
    update: XOR<assigneesUpdateWithoutTasksInput, assigneesUncheckedUpdateWithoutTasksInput>
    create: XOR<assigneesCreateWithoutTasksInput, assigneesUncheckedCreateWithoutTasksInput>
  }

  export type assigneesUpdateWithWhereUniqueWithoutTasksInput = {
    where: assigneesWhereUniqueInput
    data: XOR<assigneesUpdateWithoutTasksInput, assigneesUncheckedUpdateWithoutTasksInput>
  }

  export type assigneesUpdateManyWithWhereWithoutTasksInput = {
    where: assigneesScalarWhereInput
    data: XOR<assigneesUpdateManyMutationInput, assigneesUncheckedUpdateManyWithoutTasksInput>
  }

  export type assigneesScalarWhereInput = {
    AND?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
    OR?: assigneesScalarWhereInput[]
    NOT?: assigneesScalarWhereInput | assigneesScalarWhereInput[]
    userId?: IntFilter<"assignees"> | number
    taskId?: IntFilter<"assignees"> | number
  }

  export type usersUpsertWithoutTasksInput = {
    update: XOR<usersUpdateWithoutTasksInput, usersUncheckedUpdateWithoutTasksInput>
    create: XOR<usersCreateWithoutTasksInput, usersUncheckedCreateWithoutTasksInput>
    where?: usersWhereInput
  }

  export type usersUpdateToOneWithWhereWithoutTasksInput = {
    where?: usersWhereInput
    data: XOR<usersUpdateWithoutTasksInput, usersUncheckedUpdateWithoutTasksInput>
  }

  export type usersUpdateWithoutTasksInput = {
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    assignees?: assigneesUpdateManyWithoutUsersNestedInput
  }

  export type usersUncheckedUpdateWithoutTasksInput = {
    id?: IntFieldUpdateOperationsInput | number
    name?: NullableStringFieldUpdateOperationsInput | string | null
    image?: NullableStringFieldUpdateOperationsInput | string | null
    password?: StringFieldUpdateOperationsInput | string
    refreshToken?: NullableStringFieldUpdateOperationsInput | string | null
    username?: StringFieldUpdateOperationsInput | string
    assignees?: assigneesUncheckedUpdateManyWithoutUsersNestedInput
  }

  export type assigneesCreateWithoutUsersInput = {
    tasks: tasksCreateNestedOneWithoutAssigneesInput
  }

  export type assigneesUncheckedCreateWithoutUsersInput = {
    taskId: number
  }

  export type assigneesCreateOrConnectWithoutUsersInput = {
    where: assigneesWhereUniqueInput
    create: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput>
  }

  export type assigneesCreateManyUsersInputEnvelope = {
    data: assigneesCreateManyUsersInput | assigneesCreateManyUsersInput[]
    skipDuplicates?: boolean
  }

  export type tasksCreateWithoutCreatorInput = {
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    isDone?: boolean
    assignees?: assigneesCreateNestedManyWithoutTasksInput
  }

  export type tasksUncheckedCreateWithoutCreatorInput = {
    id?: number
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    isDone?: boolean
    assignees?: assigneesUncheckedCreateNestedManyWithoutTasksInput
  }

  export type tasksCreateOrConnectWithoutCreatorInput = {
    where: tasksWhereUniqueInput
    create: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput>
  }

  export type tasksCreateManyCreatorInputEnvelope = {
    data: tasksCreateManyCreatorInput | tasksCreateManyCreatorInput[]
    skipDuplicates?: boolean
  }

  export type assigneesUpsertWithWhereUniqueWithoutUsersInput = {
    where: assigneesWhereUniqueInput
    update: XOR<assigneesUpdateWithoutUsersInput, assigneesUncheckedUpdateWithoutUsersInput>
    create: XOR<assigneesCreateWithoutUsersInput, assigneesUncheckedCreateWithoutUsersInput>
  }

  export type assigneesUpdateWithWhereUniqueWithoutUsersInput = {
    where: assigneesWhereUniqueInput
    data: XOR<assigneesUpdateWithoutUsersInput, assigneesUncheckedUpdateWithoutUsersInput>
  }

  export type assigneesUpdateManyWithWhereWithoutUsersInput = {
    where: assigneesScalarWhereInput
    data: XOR<assigneesUpdateManyMutationInput, assigneesUncheckedUpdateManyWithoutUsersInput>
  }

  export type tasksUpsertWithWhereUniqueWithoutCreatorInput = {
    where: tasksWhereUniqueInput
    update: XOR<tasksUpdateWithoutCreatorInput, tasksUncheckedUpdateWithoutCreatorInput>
    create: XOR<tasksCreateWithoutCreatorInput, tasksUncheckedCreateWithoutCreatorInput>
  }

  export type tasksUpdateWithWhereUniqueWithoutCreatorInput = {
    where: tasksWhereUniqueInput
    data: XOR<tasksUpdateWithoutCreatorInput, tasksUncheckedUpdateWithoutCreatorInput>
  }

  export type tasksUpdateManyWithWhereWithoutCreatorInput = {
    where: tasksScalarWhereInput
    data: XOR<tasksUpdateManyMutationInput, tasksUncheckedUpdateManyWithoutCreatorInput>
  }

  export type tasksScalarWhereInput = {
    AND?: tasksScalarWhereInput | tasksScalarWhereInput[]
    OR?: tasksScalarWhereInput[]
    NOT?: tasksScalarWhereInput | tasksScalarWhereInput[]
    id?: IntFilter<"tasks"> | number
    title?: StringNullableFilter<"tasks"> | string | null
    created_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    due_at?: DateTimeNullableFilter<"tasks"> | Date | string | null
    creatorid?: IntFilter<"tasks"> | number
    isDone?: BoolFilter<"tasks"> | boolean
  }

  export type assigneesCreateManyTasksInput = {
    userId: number
  }

  export type assigneesUpdateWithoutTasksInput = {
    users?: usersUpdateOneRequiredWithoutAssigneesNestedInput
  }

  export type assigneesUncheckedUpdateWithoutTasksInput = {
    userId?: IntFieldUpdateOperationsInput | number
  }

  export type assigneesUncheckedUpdateManyWithoutTasksInput = {
    userId?: IntFieldUpdateOperationsInput | number
  }

  export type assigneesCreateManyUsersInput = {
    taskId: number
  }

  export type tasksCreateManyCreatorInput = {
    id?: number
    title?: string | null
    created_at?: Date | string | null
    due_at?: Date | string | null
    isDone?: boolean
  }

  export type assigneesUpdateWithoutUsersInput = {
    tasks?: tasksUpdateOneRequiredWithoutAssigneesNestedInput
  }

  export type assigneesUncheckedUpdateWithoutUsersInput = {
    taskId?: IntFieldUpdateOperationsInput | number
  }

  export type assigneesUncheckedUpdateManyWithoutUsersInput = {
    taskId?: IntFieldUpdateOperationsInput | number
  }

  export type tasksUpdateWithoutCreatorInput = {
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
    assignees?: assigneesUpdateManyWithoutTasksNestedInput
  }

  export type tasksUncheckedUpdateWithoutCreatorInput = {
    id?: IntFieldUpdateOperationsInput | number
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
    assignees?: assigneesUncheckedUpdateManyWithoutTasksNestedInput
  }

  export type tasksUncheckedUpdateManyWithoutCreatorInput = {
    id?: IntFieldUpdateOperationsInput | number
    title?: NullableStringFieldUpdateOperationsInput | string | null
    created_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    due_at?: NullableDateTimeFieldUpdateOperationsInput | Date | string | null
    isDone?: BoolFieldUpdateOperationsInput | boolean
  }



  /**
   * Aliases for legacy arg types
   */
    /**
     * @deprecated Use TasksCountOutputTypeDefaultArgs instead
     */
    export type TasksCountOutputTypeArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = TasksCountOutputTypeDefaultArgs<ExtArgs>
    /**
     * @deprecated Use UsersCountOutputTypeDefaultArgs instead
     */
    export type UsersCountOutputTypeArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = UsersCountOutputTypeDefaultArgs<ExtArgs>
    /**
     * @deprecated Use assigneesDefaultArgs instead
     */
    export type assigneesArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = assigneesDefaultArgs<ExtArgs>
    /**
     * @deprecated Use tasksDefaultArgs instead
     */
    export type tasksArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = tasksDefaultArgs<ExtArgs>
    /**
     * @deprecated Use usersDefaultArgs instead
     */
    export type usersArgs<ExtArgs extends $Extensions.Args = $Extensions.DefaultArgs> = usersDefaultArgs<ExtArgs>

  /**
   * Batch Payload for updateMany & deleteMany & createMany
   */

  export type BatchPayload = {
    count: number
  }

  /**
   * DMMF
   */
  export const dmmf: runtime.BaseDMMF
}