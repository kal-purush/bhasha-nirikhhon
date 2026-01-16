/**
 * A 'type A = Newtype<N, T>' is like a type synonym 'type A = T', but it
 * creates a distinct type 'A' with the same runtime representation as 'T'. That
 * is, it is a compile error to use a value of type 'A' in place of a value of
 * type 'T' and vice-versa, but the type has no boxing overhead, as it only
 * exists at compile-time.
 *
 * In order to make a new newtype, declare it as follows:
 *
 *   type Int = Newtype<{ readonly Int: unique symbol; }, number>;
 *
 * This declares 'Int' as a distinct type with the same representation as
 * 'number'. In order to convert between the two types, you need an isomorphism:
 *
 *   // Make the newtype converter
 *   const Int = newtype<Int>();
 *
 *   // Wrap a 'number' into an 'Int'
 *   const anInt: Int = Int.wrap(3);
 *
 *   // Unwrap an 'Int' into a 'number'
 *   const aNumber: number = Int.unwrap(anInt);
 *
 *   // Error! Type 'number' is not assignable to type 'Int'
 *   const error: Int = aNumber;
 *
 * Using this strategy, the type and the converter can both have the same name.
 */
export interface Newtype<N, T> {
    readonly _newtype: N;
}

/**
 * Extract the representation type 'T' from a 'Newtype<N, T>'.
 */
export type NewtypeRepr<N extends Newtype<any, any>> = N extends Newtype<any, infer T> ? T : never;

/**
 * Witnesses an isomorphism between two types; that is, encapsulates a way to
 * convert back and forth between 'A' and 'B', implying that the types are
 * isomorphic.
 *
 * The morphisms in either direction would normally be called 'to' and 'from',
 * but given that this is only used for wrapping and unwrapping newtypes, they
 * are simply called 'unwrap' and 'wrap' here instead.
 *
 * This is a kind of functional optic, and is a special case of a lens. For more
 * information about optics in TypeScript, look into 'monocle-ts'.
 */
export interface Iso<A, B> {
    readonly unwrap: (domain: A) => B;
    readonly wrap: (codomain: B) => A;
}

/**
 * Construct an 'Iso' that is able to convert between a newtype wrapper and its
 * representation type.
 */
export function newtype<N extends Newtype<any, any>>(): Iso<N, NewtypeRepr<N>> {
    return {
        // Since the wrapper type only carries type information, unsafe coercing
        // is fine here
        unwrap: (x: N) => x as NewtypeRepr<N>,
        wrap: (x: NewtypeRepr<N>) => x as N,
    };
}
