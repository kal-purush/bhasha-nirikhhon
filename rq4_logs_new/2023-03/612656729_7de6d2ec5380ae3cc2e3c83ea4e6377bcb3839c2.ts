import { parse } from "path"
import { parseMinMaxPlayers } from "./boardGameGeekReader.util"

describe('parseMinMaxPlayers', () => {

    it('should return two numbers if input is seperate with `-`', () => {
        const result = parseMinMaxPlayers('4-6')
        expect(result).toMatchObject([4, 6])
    })
    it('should return second number undefined when no seperator', () => {
        const result = parseMinMaxPlayers('4')
        expect(result).toMatchObject([4, undefined])
    })
    it('should return two numbers when seperator present and white space', () => {
        const result = parseMinMaxPlayers('4 - 6')
        expect(result).toMatchObject([4, 6])
    })
})