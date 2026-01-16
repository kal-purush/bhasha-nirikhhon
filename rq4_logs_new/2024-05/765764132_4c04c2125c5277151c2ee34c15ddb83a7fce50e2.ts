import { Nullable } from './types'

export function exists<T>(nullable: Nullable<T>): nullable is T {
    return nullable !== null && nullable !== undefined
}

export const limitNumberByMin = (value: number, min = 0) =>
    value < min ? min : value
export const limitNumberByMax = (value: number, max: number) =>
    value > max ? max : value
export const getRandomInt = (min: number, max: number) => {
    const minCeiled = Math.ceil(min)
    const maxFloored = Math.floor(max)
    return Math.floor(Math.random() * (maxFloored - minCeiled) + minCeiled)
}
export const getRandomNumbers = (min: number, max: number): number[] => {
    const numbers = new Set<number>()

    while (numbers.size < 8) {
        const randomNum = getRandomInt(min, max)
        numbers.add(randomNum)
    }

    return Array.from(numbers)
}