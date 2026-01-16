import type { ZodEffects, ZodSchema, ZodTypeAny } from "zod"

/**
 * An interface which defines a method of parsing that returns either an output type or `undefined`.
 */
export interface OptionalParseable<Input, Output> {
  parse(input: Input): Output | undefined
}

declare module "zod" {
  export namespace z {
    /**
     * Creates a zod schema from an object which comforms to the {@link OptionalParseable} interface.
     *
     * This is mostly useful for createing Zod Schemas for rich doamin types like {@link UserHandle},
     * which take in a string input, but want an output of themself.
     *
     * Ex.
     * ```ts
     * class DataType {
     *   static parse (rawValue: string): DataType | undefined {
     *     // Validate that the string is indeed a valid "DataType" then return an instance of "DataType"
     *     // if so, else undefined. ...
     *   }
     *
     *   static zodSchema = z.optionalParseable(DataType)
     * }
     * ```
     *
     * @param parseable see {@link OptionalParseable}
     * @param errorMessage a function that gets the error message when parsing fails.
     * @returns a zod schema that wraps the parseable.
     */
    function optionalParseable<Input, Output>(
      parseable: OptionalParseable<Input, Output>,
      errorMessage?: (input: Input) => string
    ): ZodEffects<ZodTypeAny, Output, Input>

    /**
     * Infers a zod schema as a "Readonly" type.
     */
    type rInfer<Schema extends ZodSchema> = Readonly<z.infer<Schema>>
  }
}