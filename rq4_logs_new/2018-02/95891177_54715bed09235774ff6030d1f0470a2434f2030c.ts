import * as log from "electron-log"
import { Position } from "lib/Kifu"

export function exportKif(game: Position[]): string {
    const header = ["V2.2", "PI", "+"]
    const moves = game.slice(1).map(p => {
        const { piece, promote } = p.lastMove
        if (!piece) return
        const kifPiece = getKifPiece(piece.toLowerCase(), promote)
        const turn = p.nextColor === "w" ? "+" : "-"
        let from = "00"
        if (p.lastMove.from) {
            const { x, y } = p.lastMove.from
            from = `${x}${y}`
        }
        const to = `${p.lastMove.to.x}${p.lastMove.to.y}`
        return `${turn}${from}${to}${kifPiece}`
    }).filter(v => v) as string[]
    return header.concat(moves).join("\n")
}

function getKifPiece(piece: string, promote: boolean): string {
    if (!promote) {
        return kifPieceMap.get(piece) || ""
    } else {
        return kifPieceMapPromote.get(piece) || ""
    }
}

const kifPieceMap = new Map<string, string>([
    ["p", "FU"],
    ["l", "KY"],
    ["n", "KE"],
    ["s", "GI"],
    ["g", "KI"],
    ["b", "KA"],
    ["r", "HI"],
    ["k", "OU"],
    ["+p", "TO"],
    ["+l", "NY"],
    ["+n", "NK"],
    ["+s", "NG"],
    ["+b", "UM"],
    ["+r", "RY"]
])

const kifPieceMapPromote = new Map<string, string>([
    ["p", "TO"],
    ["l", "NY"],
    ["n", "NK"],
    ["s", "NG"],
    ["b", "UM"],
    ["r", "RY"]
])