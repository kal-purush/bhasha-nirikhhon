import { existsSync, mkdirSync, rmSync } from "fs";
import { unlink, writeFile } from "fs/promises";

export default class LocalFileService {
  static baseUrl = "./public/local/";

  static getSrc(scope: string, name: string) {
    return `/local/${scope}/${name}`;
  }

  static createFromBase64(
    base64: string,
    scope: string,
    name: string,
  ): Promise<string> {
    const folderPath = `${this.baseUrl}${scope}`;
    const fullPath = `${folderPath}/${name}`;
    if (!existsSync(folderPath)) {
      mkdirSync(folderPath, { recursive: true });
    }
    if (existsSync(fullPath)) {
      throw new Error("File already exists");
    }
    const base64Data = base64.replace(/^data:image\/\w+;base64,/, "");
    const buffer = Buffer.from(base64Data, "base64");
    return writeFile(fullPath, buffer)
      .then((res) => res)
      .then(() => this.getSrc(scope, name));
  }

  static delete(scope: string, name: string) {
    return unlink(`${this.baseUrl}${scope}/${name}`);
  }

  static clean(scope?: string) {
    const path = `./public/local${scope ? `/${scope}` : ""}`;
    if (existsSync(path)) {
      rmSync("./public/local", { recursive: true });
    }
  }
}