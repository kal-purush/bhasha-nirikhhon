import { join } from "path"

import { BoilerRecord } from "."
import boilerActions from "./boilerActions"
import boilerAnswers from "./boilerAnswers"
import boilerFiles from "./boilerFiles"
import boilerPaths from "./boilerPaths"
import git from "./git"
import packages, {
  PackageRecord,
  FindOptions,
} from "./packages"

export class BoilerPackages {
  extractName(nameOrPathOrRepo: string): string {
    const nameMatch = nameOrPathOrRepo.match(
      /([^\/.]+)\.*[git/]*$/
    )

    if (!nameMatch) {
      console.error(
        "Argument `${repo}` should be a name, path, or repository."
      )
      process.exit(1)
    }

    return nameMatch[1]
  }

  async find(
    cwdPath: string,
    args: string[],
    options: FindOptions = {}
  ): Promise<BoilerRecord[]> {
    return (await packages.find(cwdPath, args, {
      matcher: this.matcher.bind(this),
      modify: this.modifyFind.bind(this),
      ...options,
    })) as BoilerRecord[]
  }

  async load(cwdPath: string): Promise<void> {
    await packages.load(
      cwdPath,
      join(cwdPath, ".boiler.json"),
      join(cwdPath, "boiler"),
      {
        dirsOnly: true,
        modify: this.modifyLoad.bind(this),
      }
    )
  }

  matcher(arg: string, record: PackageRecord): boolean {
    return this.extractName(arg) === record.name
  }

  async modifyLoad(
    cwdPath: string,
    record: BoilerRecord
  ): Promise<BoilerRecord> {
    const {
      answers,
      arg,
      id,
      name,
      newRecord,
      repo,
    } = record

    if (newRecord) {
      record.repo = arg
      record.name = this.extractName(arg)
    } else {
      record.repo = repo
      record.name = name || this.extractName(repo)
    }

    record.answers = boilerAnswers.load(
      cwdPath,
      id,
      answers
    )

    return record
  }

  async modifyFind(
    cwdPath: string,
    record: BoilerRecord
  ): Promise<BoilerRecord> {
    record = await this.modifyLoad(cwdPath, record)

    const { name, repo } = record

    if (!repo) {
      const { out } = await git.remote(
        join(cwdPath, "boiler", name)
      )

      record.repo = out.trim()
      record.name = this.extractName(record.repo)
    }

    record.paths = await boilerPaths.load(
      cwdPath,
      record.name
    )

    if (!record.paths.boilerDirExists) {
      return record
    }

    record.files = await boilerFiles.load(cwdPath, record)

    return record
  }

  async modifySave(
    cwdPath: string,
    record: BoilerRecord
  ): Promise<BoilerRecord> {
    const { id, name, repo, writes } = record

    return {
      answers: boilerAnswers.load(cwdPath, id),
      repo,
      writes:
        boilerActions.writes(cwdPath, record) || writes,
      version: await git.commitHash(
        join(cwdPath, "boiler", name)
      ),
    }
  }

  async reload(
    cwdPath: string,
    records: PackageRecord[]
  ): Promise<BoilerRecord[]> {
    return (await packages.reload(cwdPath, records, {
      modify: this.modifyFind.bind(this),
    })) as BoilerRecord[]
  }

  async save(cwdPath: string): Promise<void> {
    await packages.save(
      cwdPath,
      join(cwdPath, ".boiler.json"),
      { modify: this.modifySave.bind(this) }
    )
  }
}

export default new BoilerPackages()