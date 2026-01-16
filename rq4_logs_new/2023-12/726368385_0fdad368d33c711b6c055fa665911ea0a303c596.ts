import { readFileSync } from 'fs'
import { join } from 'path'

const file = join(__dirname, './input.txt')

const GALAXY = '#'
const EMPTY = '.'
const EXPAND_RATE = 1_000_000

const map = readFileSync(file)
  .toString('utf8')
  .split('\n')
  .filter(Boolean)
  .map((y) => y.split(''))

const emptyRows = (function findEmptyRows() {
  const emptyRowIndexes: number[] = []
  for (let i = 0; i < map.length; i++) {
    if (!map[i].includes(GALAXY)) {
      emptyRowIndexes.push(i)
    }
  }
  return emptyRowIndexes
})()

const emptyColumns = (function findEmptyColumns() {
  const emptyColumnIndexes: number[] = []
  for (let i = 0; i < map[0].length; i++) {
    if (map.every((row) => row[i] === EMPTY)) {
      emptyColumnIndexes.push(i)
    }
  }
  return emptyColumnIndexes
})()

type Point = { x: number; y: number }

const flatGalaxies: Point[] = []
const galaxies: (Point | null)[][] = (function mapToPoints() {
  return map.map((row, y) =>
    row.map((cell, x) => {
      if (cell === GALAXY) {
        const galaxy = { x, y }
        flatGalaxies.push(galaxy)
        return galaxy
      }
      return null
    })
  )
})()

;(function expandUniverse() {
  ;(function expandByRows() {
    let shift = 0
    galaxies.forEach((row, y) => {
      if (emptyRows.includes(y)) {
        shift += EXPAND_RATE - 1
      } else {
        row.filter(Boolean).forEach((point) => {
          point!.y += shift
        })
      }
    })
  })()
  ;(function expandByColumns() {
    let shift = 0
    Array.from({ length: galaxies[0].length })
      .map((_, i) => i)
      .forEach((x) => {
        if (emptyColumns.includes(x)) {
          shift += EXPAND_RATE - 1
        } else {
          galaxies
            .map((row) => row[x])
            .filter(Boolean)
            .forEach((point) => {
              point!.x += shift
            })
        }
      })
  })()
})()

const manhattanDistance = (a: Point, b: Point): number =>
  Math.abs(a.x - b.x) + Math.abs(a.y - b.y)

function* getDistances(): Generator<number> {
  for (let i = 0; i < flatGalaxies.length; i++) {
    for (let j = i + 1; j < flatGalaxies.length; j++) {
      yield manhattanDistance(flatGalaxies[i], flatGalaxies[j])
    }
  }
}

const result = (function findMinimalDistance() {
  let sum = 0
  for (const distance of getDistances()) {
    sum += distance
  }
  return sum
})()

console.log('Result:', result)