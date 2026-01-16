import { PlorthWord } from "plorth-types";

/**
 * Type declaration for module importer implementations.
 */
export default interface Importer {
  /**
   * Attempts to import a module from given URL/filename. If the import was
   * successful, words declared in the modules dictionary should be returned
   * as an array. Otherwise some kind of exception should be thrown.
   */
  import: (filename: string) => PlorthWord[];
}