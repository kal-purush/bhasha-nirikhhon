import zip from 'lodash-es/zip'
import initial from 'lodash-es/initial'
import tail from 'lodash-es/tail'

type Task = {
    pinOrder: number[]
    min: number,
    max: number,
    lines: [number, number][],
}

export function factory(text: string): Task {
    const coords = text
        .split(',')
        .map(Number)
    const min = Math.min(...coords)
    const max = Math.max(...coords)

    return {
        pinOrder: coords,
        min,
        max,
        lines: pairs(coords)
    }
}

function pairs<T>(xs: T[]): [T, T][] {
    return zip(initial(xs), tail(xs))
}