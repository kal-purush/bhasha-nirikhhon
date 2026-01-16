import type {
  Loader,
  Require,
  Define,
  Globals,
  DeprecatedGlobals
} from './types';

/**
 * Gets the `loader.js` globals.
 *
 * @param scope Optional scope to get the globals from. Defaults to `window`,
 * which is what `loader.js` uses in the standard `ember-cli` setup.
 *
 * @remarks Because it's so unlikely, that you'd need to change the scope, we're
 * re-exporting `loader, define` & `require` from `window` for convenience.
 *
 * @throws If either of `loader`, `define` or `require` are not defined on
 * `scope`.
 */
export function getGlobals<
  L extends Loader = Loader,
  R extends Require = Require,
  D extends Define = Define
>(scope: Partial<Globals<L, R, D>> = window as any): Globals<L, R, D> {
  if (!scope.loader)
    throw new TypeError(
      `loader.js: \`loader\` global is not defined on \`${scope}\`.`
    );
  if (!scope.define)
    throw new TypeError(
      `loader.js: \`loader\` global is not defined on \`${scope}\`.`
    );
  if (!scope.require)
    throw new TypeError(
      `loader.js: \`require\` global is not defined on \`${scope}\`.`
    );

  return {
    loader: scope.loader,
    define: scope.define,
    require: scope.require
  };
}

type AllowUndefined<T> = { [P in keyof T]: T[P] | undefined };

/**
 * Returns any defined `loader.js` globals, including deprecated ones.
 *
 * @deprecated Use {@link getGlobals} instead, if possible.
 *
 * @param scope Optional scope to get the globals from. Defaults to `window`,
 * which is what `loader.js` uses in the standard `ember-cli` setup.
 */
export function getAvailableGlobals<
  L extends Loader = Loader,
  R extends Require = Require,
  D extends Define = Define
>(
  scope: Partial<Globals<L, R, D>> = window as any
): AllowUndefined<DeprecatedGlobals<L, R, D>> {
  return {
    loader: scope.loader,
    define: scope.define,
    require: scope.require,
    requirejs: scope.requirejs,
    requireModule: scope.requireModule
  };
}