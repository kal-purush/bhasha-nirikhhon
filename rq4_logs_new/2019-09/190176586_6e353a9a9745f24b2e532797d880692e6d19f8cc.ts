import { BufferedLogger } from "./BufferedLogger"
import { IKey, debugLog } from "./Cli"
import chalk from "chalk"

export class SimpleSchemaViewer {
  schema: typeof import("./schema")["schema"]
  query = ""
  logger = new BufferedLogger()
  activeLineIndex = 0
  openKeys = {}

  constructor(schema) {
    this.schema = schema
    this.activeLineIndex = 0
  }

  handleKeyPress(str, key: IKey) {
    key.isChar = key.sequence.length === 1 && (key.name === undefined || key.name.length === 1)
    key.str = str

    if (key.name === "up") {
      this.activeLineIndex--
      if (this.activeLineIndex < 0) {
        this.activeLineIndex = this.lines.length - 1
      }
    } else if (key.name === "down") {
      this.activeLineIndex++
      if (this.activeLineIndex >= this.lines.length) {
        this.activeLineIndex = 0
      }
    } else if (key.name === "pageup") {
      this.activeLineIndex = 0
    } else if (key.name === "pagedown") {
      this.activeLineIndex = this.lines.length - 1
    } else if (key.name === "return") {
      const line = this.lines[this.activeLineIndex]
      if (line.handleSelect) {
        line.handleSelect()
      } else {
        this.toggleOpen(this.lines[this.activeLineIndex].key)
      }
    } else {
      if (key.name === "backspace") {
        this.query = this.query.slice(0, -1)
      } else if (key.isChar) {
        this.query += key.str
      }
    }

    // debugLog("want to render")

    this.reRender()
  }

  isOpen(key) {
    return this.openKeys[key] || false
  }
  toggleOpen(key) {
    this.openKeys[key] = !this.openKeys[key]
    this.reRender()
  }

  get lines(): { key?: string; handleSelect(): void; text: string }[] {
    let filteredSchema = this.schema

    if (this.query) {
      filteredSchema = {
        ...this.schema,
        models: this.schema.models
          .filter(model => {
            return (
              model.name.includes(this.query) ||
              model.attributes.some(attr => attr.name.includes(this.query))
            )
          })
          .map(model => ({
            ...model,
            attributes: model.attributes.filter(attr => attr.name.includes(this.query))
          }))
      }
    }

    const lines = []

    filteredSchema.models.forEach(model => {
      const modelAttrKey = `${model.name}_attributes`
      const isModelOpen = this.query || this.isOpen(model.name)
      if (isModelOpen) {
        lines.push({ key: model.name, text: ` v ${model.name}` })

        if (this.query || this.isOpen(modelAttrKey)) {
          lines.push({ key: modelAttrKey, text: `  v attributes` })

          model.attributes.forEach(attr => {
            lines.push({
              handleSelect: () => {
                debugLog("SELECT")
                this.query = ""
                this.activeLineIndex = this.lines.findIndex(line => line.key === attr.data.model)
              },
              text: `    ${attr.name} (${attr.type})`
            })
          })
        } else {
          lines.push({ key: modelAttrKey, text: `  > attributes` })
        }
        lines.push({ key: `${model.name}_scopes`, text: "    scopes" })
        lines.push({ key: `${model.name}_permissions`, text: "    permissions" })
        // model.attributes.forEach(attribute => {})
      } else {
        lines.push({ key: model.name, text: ` > ${model.name}` })
      }
    })

    return lines
  }
  reRender() {
    const buffer = this.logger.newBuffer()

    for (const index in this.lines) {
      if (index === String(this.activeLineIndex)) {
        buffer.print(chalk.bgGreen(" "))
      } else {
        buffer.print(" ")
      }
      const line = this.lines[index]
      buffer.println(line.text)
    }

    buffer.println("---------------------")
    buffer.println(chalk.bgWhite(chalk.black("Query:")) + " " + this.query)

    buffer.flush({ full: true })
  }
}