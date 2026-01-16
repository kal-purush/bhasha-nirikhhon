type Primitive = string | number | boolean | null | undefined;

type InferLiteral<T> = T extends Primitive ? T : never;

/**
 * Recursively infers the most specific literal type for given input.
 * For primitives, it directly infers the literal type.
 * For arrays, it infers each element.
 * For objects, it infers each property.
 * 
 * Ex.
 * ```ts
 * declare function literal<T>(options: Literal<T>): T;
 * const complexObject = literal({x: {y: 3, z: [1, 2, 3]}, w: new Date(2000)})
 * // typeof complexObject is inferred the literal values
 * ```
 */
export type Literal<T> = T extends Primitive ? InferLiteral<T> :
  T extends Array<infer U> ? Array<Literal<U>> :
  {
    [P in keyof T]-?: Literal<T[P]>;
  };

// Example Usage:
declare function literal<T>(options: Literal<T>): T;

// typeof complexObject is inferred with literal values
const complexObject = literal({x: {y: 3, z: [1, 2, 3]}, w: new Date(2000)})