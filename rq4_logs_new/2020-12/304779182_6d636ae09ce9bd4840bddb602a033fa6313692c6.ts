import path = require("path");

/**
 * Wrapper function to extract filename.ext from '`${fileName(__filename)}`'
 * @param fullPath `${fileName(__filename)}`
 */
export function fileName(fullPath: string) {
  return path.basename(fullPath);
}