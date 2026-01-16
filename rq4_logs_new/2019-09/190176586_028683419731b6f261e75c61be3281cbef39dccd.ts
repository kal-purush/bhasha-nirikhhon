import ansi from "ansi"
import { appendFileSync, writeFileSync } from "fs"
const cursor = ansi(process.stdout)

const debug = (...data) => appendFileSync("./debug.txt", `${JSON.stringify(data)}\n`)
writeFileSync("./debug.txt", ``)
export interface IKey {
  str: string
  sequence: string
  name: "backspace" | "up" | "down" | "left" | "right" | "return"
  ctrl: boolean
  meta: boolean
  shift: boolean
  isChar: boolean
}

export abstract class Line {
  cli: Cli
  isActive = false

  setActive(state) {
    this.isActive = state
  }

  handleInput(key: IKey) {}

  onAfterMount() {}
  onUnmount() {}
  abstract render(): string
}

export class Cli {
  clearScreen: boolean
  debug: boolean
  lines: Line[] = []
  activeLineIndex = 0
  scrollPos = 0

  constructor({
    debug = false,
    clearScreen = true,
    lines = []
  }: {
    debug?: boolean
    clearScreen?: boolean
    lines?: Line[]
  }) {
    this.registerLines(lines)
    this.lines = lines
    this.clearScreen = clearScreen
    this.debug = debug
  }

  get height() {
    return this.debug ? process.stdout.rows - 5 : process.stdout.rows
  }

  registerLines(lines: Line[]) {
    lines.forEach(line => (line.cli = this))
  }

  moveActiveLine(delta: number) {
    this.changeActiveLineIndex(this.activeLineIndex + delta)
  }
  changeActiveLineIndex(to: number, opts: { scroll?: "middle" | "visible" } = {}) {
    this.activeLineIndex = to

    // clamp index between 0 and line
    if (this.activeLineIndex < 0) {
      this.activeLineIndex = 0
    } else if (this.activeLineIndex >= this.lines.length) {
      this.activeLineIndex = this.lines.length - 1
    }

    // update which line is active
    this.lines.filter(line => line.setActive).forEach((line, i) => line.setActive(false))
    const newCurrent = this.lines[this.activeLineIndex]
    if (newCurrent.setActive) {
      this.lines[this.activeLineIndex].setActive(true)
    }

    const scrollTop = this.scrollPos
    const scrollBottom = this.scrollPos + this.height - 1

    debug("checking scroll")
    //set scroll position
    if (opts.scroll === "middle") {
      this.scrollPos = this.activeLineIndex - this.height / 2
    } else {
      if (scrollTop > this.activeLineIndex) {
        debug("scrolling up")
        this.scrollPos = this.activeLineIndex
      } else if (scrollBottom < this.activeLineIndex) {
        debug("scrolling down", this.scrollPos, this.activeLineIndex)
        this.scrollPos = this.activeLineIndex - this.height + 1
      }
    }

    // clamp scrolling
    if (this.scrollPos > this.lines.length) {
      this.scrollPos = this.lines.length - this.height
    }
    if (this.scrollPos < 0) {
      this.scrollPos = 0
    }
  }
  setActiveLine(line: Line) {
    const idx = this.lines.indexOf(line)
    if (idx === -1) {
      throw "that line is not on the screen"
    } else {
      this.changeActiveLineIndex(idx, { scroll: "middle" })
    }
  }

  addLinesAfter(line: Line, lines: Line[]) {
    const idx = this.lines.indexOf(line)
    if (idx === -1) {
      throw "that line is not on the screen"
    } else {
      lines.forEach((line, i) => {
        this.addLine(line, idx + i + 1)
      })
    }
  }

  addLine(line: Line, index: number) {
    if (this.lines.includes(line)) {
      throw "trying to add line that is already mounted"
    }
    this.registerLines([line])
    this.lines.splice(index, 0, line)
    line.onAfterMount()
  }
  removeLines(lines: Line[]) {
    lines.forEach(line => {
      this.removeLine(line)
    })
  }
  removeLine(line: Line) {
    const idx = this.lines.indexOf(line)
    if (idx > -1) {
      line.onUnmount()
      this.lines.splice(idx, 1)
    }
  }

  handleKeyPress(str, key: IKey) {
    key.isChar = key.sequence.length === 1 && (key.name === undefined || key.name.length === 1)
    key.str = str

    if (key.name === "up") {
      this.moveActiveLine(-1)
    } else if (key.name === "down") {
      this.moveActiveLine(1)
    } else {
      const line = this.lines[this.activeLineIndex]
      if (line.handleInput) {
        line.handleInput(key)
      }
    }

    this.renderScreen()
  }

  renderScreen() {
    if (this.clearScreen) {
      console.clear()
      process.stdout.write("\x1b[3J")
    } else {
      console.log()
      console.log(" ============================================ ")
      console.log()
    }

    const linesToRender = this.lines.slice(this.scrollPos, this.scrollPos + this.height)

    linesToRender.map((line, i) => {
      if (i + this.scrollPos === this.activeLineIndex) {
        cursor.write("> ")
      } else {
        cursor.write("  ")
      }
      if (line.render) {
        const content = line.render()
        console.log(line.render())
      } else {
        throw "line must define a render method"
      }
    })
    if (this.debug) {
      console.log()
      console.log()
      console.log({ scroll: this.scrollPos, index: this.activeLineIndex })
    }
  }
}