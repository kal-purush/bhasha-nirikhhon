import { Prefixed } from '@stitches/react/types/util';
import { config, type CSS } from "../stitches.config";

const getVariant = <
  K extends keyof typeof config.theme,
  T extends keyof typeof config.theme[K],
  P extends Prefixed<'$', T>,
  R extends Record<T, CSS>
>(
  prop: K,
  map: (tokenValue: P) => CSS
): R => {
  const values = Object.keys(config.theme[prop]) as T[];
  return values.reduce<R>(
    (acc, tokenValue) => ({ ...acc, [tokenValue]: map(`$${String(tokenValue)}` as P) }),
    {} as R
  );
};

export { getVariant }