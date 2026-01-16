import { join } from "path"
import { readJson, pathExists } from "fs-extra"

import npm from "./npm"

export interface BoilerPackageRecord {
  dev: string[]
  prod: string[]
}

export class BoilerPackages {
  records: Record<string, BoilerPackageRecord> = {}

  load(cwdPath: string): BoilerPackageRecord {
    if (!this.records[cwdPath]) {
      this.records[cwdPath] = { dev: [], prod: [] }
    }
    return this.records[cwdPath]
  }

  async install(cwdPath: string): Promise<void> {
    if (this.records[cwdPath]) {
      const pkgJsonPath = join(cwdPath, "package.json")
      let pkgJson = {}

      if (await pathExists(pkgJsonPath)) {
        pkgJson = await readJson(pkgJsonPath)
      }

      let { dev, prod } = this.records[cwdPath]

      dev = dev.filter(
        pkgName =>
          !pkgJson["devDependencies"] ||
          !pkgJson["devDependencies"][pkgName]
      )

      prod = prod.filter(
        pkgName =>
          !pkgJson["dependencies"] ||
          !pkgJson["dependencies"][pkgName]
      )

      await npm.install(cwdPath, dev, {
        saveDev: true,
      })

      await npm.install(cwdPath, prod)
    }
  }
}

export default new BoilerPackages()