import { createDir, readDir, BaseDirectory } from "@tauri-apps/api/fs";

export async function createDirAsync() {
  await createDir("users", { dir: BaseDirectory.App, recursive: true });
}

export async function readDirAsync() {
  try {
    const entries = await readDir("users", {
      dir: BaseDirectory.App,
      recursive: true
    });

    if (entries.length > 0)
      return entries.map(entry => ({
        name: entry,
        path: `users/${entry}`
      }));
    else return [];
  } catch (error) {
    console.error(error);
    throw error;
  }
}