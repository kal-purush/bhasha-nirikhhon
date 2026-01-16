import { doubleArgument } from '../../src/argumentTypes/doubleArgument'

describe('Double', () => {

    describe('isType()', () => {
        it('same type lower case', () => {
            expect(doubleArgument.isType('double')).toBeTruthy()
        })
        it('same type upper case', () => {
            expect(doubleArgument.isType('DOUBLE')).toBeTruthy()
        })
        it('same type pascal case', () => {
            expect(doubleArgument.isType('Double')).toBeTruthy()
        })
        it('same type with space', () => {
            expect(doubleArgument.isType(' double ')).toBeTruthy()
        })
        it('same type optional', () => {
            expect(doubleArgument.isType('double?')).toBeTruthy()
        })
        it('different type', () => {
            expect(doubleArgument.isType('string')).toBeFalsy()
        })
        it('different type but contains type', () => {
            expect(doubleArgument.isType('doubleDouble')).toBeFalsy()
        })
    })

    describe('bve5TestValue', () => {
        it('double value', () => {
            const numberRegex = /^[+,-]?([1-9]\d*|0)(\.\d+)?$/
            expect(numberRegex.test(doubleArgument.bve5TestValue)).toBeTruthy()
        })
    })

    describe('csharpTestValue', () => {
        it('double csharp value', () => {
            const numberRegex = /^[+,-]?([1-9]\d*|0)(\.\d+)?$/
            expect(numberRegex.test(doubleArgument.csharpTestValue)).toBeTruthy()
        })
    })
})