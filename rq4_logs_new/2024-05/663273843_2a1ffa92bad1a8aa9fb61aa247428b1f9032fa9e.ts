import { existsSync, mkdirSync } from "fs";
import { unlink, writeFile } from "fs/promises";
import { UTApi } from "uploadthing/server";
import type { FileEsque } from "uploadthing/types";
import { env } from "~/env";
import { fileHelper } from "~/lib/utils";

const defaultImage = "https://picsum.photos/200";

const modes = {
  local: "local",
  remote: "remote",
} as const;

const mode =
  env.UPLOADTHING_SECRET && env.UPLOADTHING_APP_ID ? modes.remote : modes.local;
const utapi =
  env.UPLOADTHING_SECRET && env.UPLOADTHING_APP_ID
    ? new UTApi({
        apiKey: env.UPLOADTHING_SECRET,
        defaultKeyType: "customId",
      })
    : null;

const localPath = "/local/things";
const remotePath = `https://utfs.io/a/${env.UPLOADTHING_APP_ID}`;

export class ImageService {
  static mode = mode;
  static baseUrl = this.mode === modes.local ? localPath : remotePath;
  static fileBaseUrl = `./public${localPath}`;

  static async createFromBase64(base64: string, id: string): Promise<string> {
    try {
      if (this.mode === modes.local) {
        if (mode === modes.local && !existsSync(this.fileBaseUrl)) {
          mkdirSync(this.fileBaseUrl, { recursive: true });
        }
        const base64Data = base64.replace(/^data:image\/\w+;base64,/, "");
        const buffer = Buffer.from(base64Data, "base64");
        const url = `${this.fileBaseUrl}/${id}`;
        return writeFile(url, buffer)
          .then((res) => res)
          .then(() => url);
      }
      if (utapi) {
        const file = fileHelper.fromBase64(base64, id) as FileEsque;
        file.customId = id;
        return utapi.uploadFiles(file).then(() => `${this.baseUrl}/${id}`);
      }
      throw new Error("Couldn't create image");
    } catch (err) {
      console.log({ err });
      return defaultImage;
    }
  }

  static async deleteImage(id: string): Promise<{ success: boolean }> {
    if (this.mode === modes.local) {
      try {
        await unlink(`${this.fileBaseUrl}/${id}`);
        return { success: true };
      } catch (err) {
        return { success: false };
      }
    }

    return !!utapi ? utapi.deleteFiles(id) : { success: false };
  }

  static getUrls(ids: string[]) {
    const urlsMap = new Map<string, string>();
    ids.forEach((id) => urlsMap.set(id, `${this.baseUrl}/${id}`));
    return urlsMap;
  }

  static getUrl(id: string) {
    return `${this.baseUrl}/${id}`;
  }
}