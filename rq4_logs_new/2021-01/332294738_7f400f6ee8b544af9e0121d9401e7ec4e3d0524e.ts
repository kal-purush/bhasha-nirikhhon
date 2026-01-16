import * as esbuild from "esbuild";

/**
 * Tell esbuild that some external modules equate to corresponding global
 * variables and avoid them to be bundled.
 *
 * @param globals
 * Mapping from module paths to variable names, e.g. `jquery: "$"`.
 */
export const globalExternals = (
  globals: Record<string, string>
): esbuild.Plugin => {
  const pluginName = "global-externals";

  const modulePaths = Object.keys(globals);
  const moduleFilter = new RegExp(`^(?:${modulePaths.join("|")})$`);

  return {
    name: pluginName,
    setup(build) {
      build.onResolve({ filter: moduleFilter }, (args) => ({
        path: args.path,
        namespace: pluginName,
      }));

      build.onLoad({ filter: /.*/, namespace: pluginName }, (args) => {
        const variableName = globals[args.path];
        if (!variableName) {
          // Shouldn't happen
          throw new Error(`Unknown module path: ${args.path}`);
        }

        return { pluginName, contents: `export default ${variableName};` };
      });
    },
  };
};