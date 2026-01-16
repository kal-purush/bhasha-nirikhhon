const mergeFlags = (...regexes: RegExp[]) => regexes.map(r => r.flags).filter((f, i, a) => a.indexOf(f) === i).join("")

// concatenate Regexes
export const concat = (...regexes: RegExp[]) => new RegExp(regexes.map(r => r.source).join(""), mergeFlags(...regexes))

// create named capture group
export const capture = (groupName: string, regex: RegExp) => new RegExp(`(?<${groupName}>${regex.source})`, regex.flags)

export const nonCapture = (regex: RegExp) => new RegExp(`(?:${regex.source})`, regex.flags)

// make a regex optional
export const optional = (regex: RegExp) => new RegExp(`(?:${regex.source})?`, regex.flags)

// make and anternation between 2 or more regexes
export const or = (...regexes: RegExp[]) => new RegExp(regexes.map(r => `${r.source}`).join("|"), mergeFlags(...regexes))

// make a regex that matches 1 single word that belongs to the wordList
export const fromList = (wordList: string[], flags?: string) => new RegExp(`[^\\d\\s\\w]*(?:${wordList.map(word => `${word}`).join("|")})[^\\d\\s\\w]*`, flags)

export const optionalSpace = /\s*/