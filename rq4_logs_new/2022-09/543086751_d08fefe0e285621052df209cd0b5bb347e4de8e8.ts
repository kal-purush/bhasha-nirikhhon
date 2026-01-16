import fs from "node:fs/promises";

export function deleteFile(filePath: string) {
  return fs.unlink(filePath);
}

export function deleteFiles(filePaths: Array<string>) {
  return Promise.all(filePaths.map(deleteFile));
}