import xs, { MemoryStream } from 'xstream'
import { run } from '@cycle/run'
import * as R from 'ramda'
import { 
    DOMSource, 
    VNode,
    makeDOMDriver,
    div,
    h1,
    p,
    button,
    span
} from '@cycle/dom';

type Grid = number[][]

const generateGrid = (gridSize: number): Grid => {
    const arr = [...Array(gridSize)]
    const row: number[] = arr.fill(0)
    const grid: number[][] = arr.fill(row)
    return grid
}

const padTo = <T>(length: number, padWith: T) => (array: T[]): T[] => {
    while (array.length < length) {
        array.push(padWith)
    }
    return array
}

const padTo4WithZeros = padTo(4, 0)

const rowReducer = R.scan(
    (x: number, y: number): number => x == y ? x + y : y, 0
)
const foldRow = (row: number[]): number[] => 
    rowReducer(row.filter(x => x > 0))

const mirrorGrid = (grid: Grid): Grid => {
    let newGrid: Grid = [[],[],[],[]]
    grid.forEach((row, idx) =>
        row.forEach((x, idx1) => 
            newGrid[idx1][idx] = grid[idx][idx1]))
    return newGrid
}
const mkFlippedReducer = (fn: (grid: Grid) => Grid) =>
    R.compose(mirrorGrid, fn, mirrorGrid)

const leftReducer = R.compose(padTo4WithZeros, foldRow)
const left = (grid: Grid): Grid => grid.map(leftReducer)
const up = mkFlippedReducer(left)

const reverse = <T>(list: T[]): T[] => list.reverse()
const rightReducer = R.compose(reverse, leftReducer, reverse)
const right = (grid: Grid): Grid => grid.map(rightReducer)
const down = mkFlippedReducer(right)

export function GameGrid(sources: { DOM: DOMSource }): { DOM: MemoryStream<VNode> } {
    const up$ = sources.DOM.select('.btn-up').events('click').mapTo(up)
    const down$ = sources.DOM.select('.btn-down').events('click').mapTo(down)
    const left$ = sources.DOM.select('.btn-left').events('click').mapTo(left)
    const right$ = sources.DOM.select('.btn-right').events('click').mapTo(right)
    const gameMove$ = xs.merge(up$, down$, left$, right$)
    const state$ = gameMove$.fold((state, reducer) => reducer(state), generateGrid(4))

    const vdom$ = state$.map(state => {
        return div('.main', [], [
            h1('2048'),
            p('Score: N/A'),
            p('High Score: N/A'),
            div('.game-container', [
                div('.grid-container', state.map(row =>
                    div('.grid-row', row.map(cell =>
                        span('.grid-element', `value: ${cell}`)
                    ))
                ))
            ]),
            button('.btn-up', 'Up'),
            button('.btn-down', 'Down'),
            button('.btn-left', 'Left'),
            button('.btn-up', 'Right')
        ])
    })
    return {
        DOM: vdom$
    }
}

const drivers = {
    DOM: makeDOMDriver('#app')
}
const main = GameGrid
run(main, drivers)