import ansi from "ansi"

const cursor = ansi(process.stdout)

export class BufferedLogger {
  lastLines?: string[]

  newBuffer() {
    let fullText = ""
    return {
      print(text: string) {
        fullText += text
      },
      println(text: string) {
        fullText += `${text}\n`
      },
      flush: () => {
        this.flush(fullText)
        fullText = ""
      }
    }
  }

  flush(text: string) {
    const lines = text.replace(/\n$/, "").split("\n")

    if (this.lastLines) {
      const diff = lines.reduce(
        (acc, line, i) => {
          const lastLine = this.lastLines[i]
          if (line !== lastLine) {
            acc[i] = line
          }
          return acc
        },
        {} as Record<number, string>
      )

      for (const index in diff) {
        cursor.goto(0, Number(index) + 1)
        cursor.eraseLine()
        cursor.write(diff[index])
      }

      let numberOfLinesRemoved = this.lastLines.length - lines.length

      while (numberOfLinesRemoved-- > 0) {
        cursor.goto(0, lines.length + numberOfLinesRemoved + 1)
        cursor.eraseLine()
      }
    } else {
      console.clear()
      process.stdout.write("\x1b[3J")

      lines.forEach((line, i) => {
        if (i !== 0) {
          cursor.write("\n")
        }
        cursor.write(line)
      })
    }

    this.lastLines = lines
  }
}