import { promises as fs } from "fs";

/**
 * Get all folder name from a parent folder
 * @param path parent folder path
 * @returns list of folder name
 */
export const getSubFoldersName = async (path: string): Promise<string[]> => {
  try {
    return await fs.readdir(path);
  } catch (error) {
    console.error(error);
    throw new Error("Failed to get folder");
  }
};

/**
 * Read file as text file
 * @param path path of the file
 * @returns content of file as string
 */
export const getFileAsString = async (path: string): Promise<string> => {
  try {
    return await fs.readFile(path, { encoding: "utf-8" });
  } catch (error) {
    console.error(error);
    throw new Error("Failed to get file");
  }
};