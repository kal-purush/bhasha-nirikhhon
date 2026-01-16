import { Boiler } from "."
import actions from "./actions"
import boilerAnswers from "./boilerAnswers"
import boilerInstances from "./boilerInstances"
import { BoilerRecord } from "./boilerRecords"

export interface BoilerAction {
  action: string
  bin?: boolean
  dev?: boolean
  path?: string
  source: any
}

export class BoilerActions {
  records: Record<string, BoilerAction[]> = {}

  async load(
    cwdPath: string,
    boiler: Boiler,
    callback: string,
    ...records: BoilerRecord[]
  ): Promise<void> {
    for (const record of records) {
      const { name } = record
      const id = `${cwdPath}:${name}`

      const instance = await boilerInstances.load(
        cwdPath,
        record
      )

      if (!instance || !instance[callback]) {
        continue
      }

      this.records[id] = await instance[callback]({
        cwdPath,
        allAnswers: boilerAnswers.allAnswers(cwdPath),
        ...record,
      })

      await actions.run(cwdPath, boiler, this.records[id])
    }
  }
}

export default new BoilerActions()