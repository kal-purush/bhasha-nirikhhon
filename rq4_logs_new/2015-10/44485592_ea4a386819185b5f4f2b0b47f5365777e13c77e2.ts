/**
 * Typings for the browserify transform tools.
 *
 * @url https://www.npmjs.com/package/browserify-transform-tools
 * @author John Vilk <jvilk@cs.umass.edu>
 */
declare module "browserify-transform-tools" {
  /**
   * Options literal for transforms.
   */
  interface IOptions {
    // A list of extensions which will not be processed, e.g. "['.coffee', '.jade']"
    excludeExtensions?: string[];
    // A list of extensions to process. If this options is not specified, then all
    // extensions will be processed. If this option is specified, then any file with
    // an extension not in this list will skipped.
    includeExtensions?: string[];
    // If set true, then your transform will only run on "javascript" files. This is
    // handy for Falafel and Require transforms, defined below. This is equivalent to
    // passing includeExtensions: [".js", ".coffee", ".coffee.md", ".litcoffee",
    // "._js", "._coffee", ".jsx", ".es", ".es6"].
    jsFilesOnly?: boolean;
  }

  /**
   * Options literal for Falafel transforms.
   */
  interface IFalafelTransformOptions extends IOptions {
    // Options passed to Falafel.
    falafelOptions?: acorn.Options;
  }

  /**
   * Options literal for require() transforms.
   */
  interface IRequireTransformOptions extends IOptions {
    // makeRequireTransform can parse many simple expressions, so the above would
    // succesfully parse require("f" + "oo"), for example. Any expression
    // involving core JavaScript, __filename, __dirname, path, and join (where
    // join is an alias for path.join) can be parsed. Setting the evaluateArguments
    // option to false will disable this behavior, in which case the source code
    // for everything inside the ()s will be returned.
    evaluateArguments: boolean;
  }

  /**
   * A Falafel AST node.
   */
  interface FalafelNode extends ESTree.Node {
    // Return the source for the given node, including any modifications made to
    // children nodes.
    source(): string;
    // Transform the source for the present node to the string s.
    update(s: string): void;
    // Reference to the parent element or null at the root element.
    parent: FalafelNode;
  }

  /**
   * Configuration data for a transform.
   */
  interface IConfigData<T> {
    // The configuration for the transform.
    config: T;
    // The directory the configuration was loaded from; the directory which contains
    // package.json if that's where the config came from, or the directory which
    // contains the file specified in package.json. This is handy for resolving
    // relative paths. Note that this field may be null if the configuration is set
    // programatically.
    configDir: string;
    // The file the configuration was loaded from. Note that this field may be null
    // if the configuration is set programatically.
    configFile: string;
    // Since a transform is run once for each file in a project, configuration data
    // is cached using the location of the package.json file as the key. If this value
    // is true, it means that data was loaded from the cache.
    cached: boolean;
    // The appliesTo property from the configuration.
    appliesTo?: IAppliesTo;
  }

  /**
   * Configures what files the transform will be applied to.
   */
  interface IAppliesTo {
    // A list of extensions to process. If this option is not specified, then all
    // extensions will be processed. If this option is specified, then any file with an
    // extension not in this list will skipped.
    includeExtensions?: string[]
    // A list of extensions which will not be processed. e.g. "['.coffee', '.jade']"
    excludeExtensions?: string[];
    // A list of paths, relative to the configuration file, of files which should be
    // transformed. Only these files will be transformed.
    files?: string[];
    // A regex or a list of regexes. If any regex matches the full path of the file,
    // then the file will be processed, otherwise not.
    regex?: string[];
  }

  /**
   * Options passed in to a transform function.
   */
  interface ITransformOptions<T> {
    // The transform's configuration data.
    configData: IConfigData<T>;
  }

  /**
   * Create a simple string transform.
   */
  function makeStringTransform<T>(name: string, options: IOptions, transform: (content: string, transformOptions: ITransformOptions<T>, done: (err: Error, newContent: string) => void) => void): any;
  /**
   * Create a transform using Falafel.
   */
  function makeFalafelTransform<T>(name: string, options: IFalafelTransformOptions, transform: (node: FalafelNode, transformOptions: ITransformOptions<T>, done: (err?: Error) => void) => void): any;
  /**
   * Create a transform that changes require() calls.
   */
  function makeRequireTransform<T>(name: string, options: IRequireTransformOptions, transform: (args: string[], options: ITransformOptions<T>, done: (err?: Error, replacement?: string) => void) => void): any;
  /**
   * Loads the named transform's configuration from the given file.
   */
  function loadTransformConfig<T>(name: string, file: string, cb: (err: Error, configData?: IConfigData<T>) => void): void;
  /**
   * Synchronous version of loadTransformConfig.
   */
  function loadTransformConfigSync<T>(name: string, file: string): IConfigData<T>;
  /**
   * Returns if the transform should skip the file based on the input configdata.
   */
  function skipFile<T>(file: string, configData: IConfigData<T>): boolean;
  /**
   * Run the named transform on the given file, and return the result to the callback.
   * Useful for unit testing.
   */
  function runTransform<T>(transform: any, file: string, config: T, cb: (err: Error, transformed?: any) => void): void;
}