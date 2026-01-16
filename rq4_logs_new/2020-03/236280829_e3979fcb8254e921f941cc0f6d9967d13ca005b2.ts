export const ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split(
  ""
)

export class TinyId {
  counter = 0

  reset(): void {
    this.counter = 0
  }

  generate(): string {
    let id = this.counter
    let s: string

    this.counter += 1

    if (id === 0) {
      return ALPHABET[0]
    }

    s = ""

    while (id > 0) {
      s += ALPHABET[id % ALPHABET.length]
      id = parseInt((id / ALPHABET.length).toString(), 10)
    }

    return s
      .split("")
      .reverse()
      .join("")
  }
}

export default new TinyId()