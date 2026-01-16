/// <reference path="../../typings/all.ts" />

var path = require('path')

var next_id = 0
class TerminalLine {
    text: string
    id: number

    constructor(text: string) {
        this.id = next_id++
        this.text = text
    }
}

class Terminal {
    session: Session
    lines: TerminalLine[]
    location: string

    constructor(session: Session) {
        this.location = session.dir
        this.lines = []
    }

    prompt_string() : string {
        return this.location
    }
}
