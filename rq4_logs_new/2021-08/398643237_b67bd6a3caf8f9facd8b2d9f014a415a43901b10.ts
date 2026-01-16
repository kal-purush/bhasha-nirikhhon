import { fileURLToPath } from "url";
import { dirname } from "path";
import callsites from "callsites";

function extractFilePathFromFileUrl(url?: string | null) {
  if (!url) {
    throw new Error("Unable to get filename");
  }

  return fileURLToPath(url);
}

export function getFilename() {
  const fileName = callsites()[1]?.getFileName();

  return extractFilePathFromFileUrl(fileName);
}

export function getDirname() {
  // Can't be reused from getFilename because that would change the callstack and make [1] not what we need.
  const fileName = callsites()[1]?.getFileName();

  return dirname(extractFilePathFromFileUrl(fileName));
}

export const __filename = getFilename;
export const __dirname = getDirname;