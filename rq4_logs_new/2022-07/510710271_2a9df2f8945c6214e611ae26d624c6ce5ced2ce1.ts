import * as O from "fp-ts/lib/Option";
import * as queryString from "query-string";
import * as t from "io-ts";
import * as E from "fp-ts/lib/Either";
import { pipe } from "fp-ts/lib/function";

export const getEnv = (name: string): O.Option<string> => {
  const v = Deno.env.get(name);
  return v ? O.some(v) : O.none;
};

export const parseQueryString =
  <P>(codec: t.Type<P>) =>
  (x: string): E.Either<unknown, P> => {
    const parsed = queryString.parse(x);
    return pipe(
      parsed.error ? E.left(parsed) : E.right(parsed),
      E.chain(codec.decode)
    );
  };