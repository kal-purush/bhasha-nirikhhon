import path from "node:path"
import { readdir, writeFile } from "node:fs/promises"
import { exists, move, rm } from "fs-extra"
import { createDirIfNotExists, getUserConfigDir } from "../utils/path"
import { responses } from "../utils/http"
// import useLogger from "../utils/logger"

// const logger = useLogger("template")

export class TemplateService {
  private static readonly rootDir = "templates"
  private static readonly suffix = ".hbs"

  configFileDir() {
    const configDir = getUserConfigDir()
    return path.resolve(configDir, TemplateService.rootDir)
  }

  async loadAll() {
    const { SUCCESS, IO_ERROR } = responses<string[]>()
    const dir = this.configFileDir()
    await createDirIfNotExists(dir)

    let templates: string[] = []
    try {
      templates = await readdir(dir)
    } catch (e) {
      return IO_ERROR.withMsg("Fail to read dir: %s", dir)
    }
    return SUCCESS.withData(templates)
  }

  async save(name: string, content: string, update = false) {
    const { SUCCESS, CONFLICT } = responses()
    const dir = this.configFileDir()
    const filePath = path.resolve(dir, name + TemplateService.suffix)
    if (await exists(filePath)) {
      if (update) {
        await writeFile(filePath, content)
      } else {
        return CONFLICT
      }
    } else {
      await writeFile(filePath, content)
    }
    return SUCCESS
  }

  async trash(name: string, physics = false) {
    const { NOT_FOUND, SUCCESS } = responses()
    const dir = this.configFileDir()
    const filePath = path.resolve(dir, name + TemplateService.suffix)
    if (!await exists(filePath)) return NOT_FOUND
    if (physics) {
      await rm(filePath)
    } else {
      await move(filePath, `${filePath}.bak`)
    }
    return SUCCESS
  }
}