import { playerCPUType } from '../types'

export const isWinningMoveCPU = (
    board: string[],
    moveIndex: number,
    symbol: string
): boolean => {
    const rows = Math.sqrt(board.length)
    const rowIndex = Math.floor(moveIndex / rows)
    const colIndex = moveIndex % rows

    let horizontalCount = 0
    for (let col = 0; col < rows; col++) {
        const index = rowIndex * rows + col
        if (board[index] === symbol) {
            horizontalCount++
            if (horizontalCount >= 5) {
                return true
            }
        } else {
            horizontalCount = 0
        }
    }

    let verticalCount = 0
    for (let row = 0; row < rows; row++) {
        const index = row * rows + colIndex
        if (board[index] === symbol) {
            verticalCount++
            if (verticalCount >= 5) {
                return true
            }
        } else {
            verticalCount = 0
        }
    }

    let diagonalCount1 = 0
    for (let i = -4; i <= 4; i++) {
        const row = rowIndex + i
        const col = colIndex + i
        if (row >= 0 && row < rows && col >= 0 && col < rows) {
            const index = row * rows + col
            if (board[index] === symbol) {
                diagonalCount1++
                if (diagonalCount1 >= 5) {
                    return true // Diagonal win
                }
            } else {
                diagonalCount1 = 0
            }
        }
    }

    let diagonalCount2 = 0
    for (let i = -4; i <= 4; i++) {
        const row = rowIndex + i
        const col = colIndex - i
        if (row >= 0 && row < rows && col >= 0 && col < rows) {
            const index = row * rows + col
            if (board[index] === symbol) {
                diagonalCount2++
                if (diagonalCount2 >= 5) {
                    return true
                }
            } else {
                diagonalCount2 = 0
            }
        }
    }

    return false
}

export const findWinningMoveForCPU = (
    cpu: playerCPUType,
    humanSymbol: string,
    board: string[],
    sqrtOfBoard: number,
    stepsAhead: number = 1
): number | null => {
    const cpuSymbol = cpu.XO

    for (let i = 0; i < board.length; i++) {
        if (board[i] === null) {
            const testBoard = [...board]
            testBoard[i] = cpuSymbol

            if (isWinningMoveCPU(testBoard, i, cpuSymbol)) {
                return i
            }
        }
    }

    for (let i = 0; i < board.length; i++) {
        if (board[i] === null) {
            const testBoard = [...board]
            testBoard[i] = humanSymbol

            if (isWinningMoveCPU(testBoard, i, humanSymbol)) {
                return i
            }
        }
    }

    if (stepsAhead > 0) {
        for (let i = 0; i < board.length; i++) {
            if (board[i] === null) {
                const testBoard = [...board]
                testBoard[i] = humanSymbol

                if (
                    findWinningMoveForCPU(
                        cpu,
                        cpuSymbol,
                        testBoard,
                        sqrtOfBoard,
                        stepsAhead - 1
                    ) !== null
                ) {
                    return i
                }
            }
        }
    }

    if (board[Math.floor(sqrtOfBoard / 2) * (sqrtOfBoard + 1)] === null) {
        return Math.floor(sqrtOfBoard / 2) * (sqrtOfBoard + 1) // Center
    }

    const corners = [
        0,
        sqrtOfBoard - 1,
        sqrtOfBoard * (sqrtOfBoard - 1),
        board.length - 1,
    ]
    for (const corner of corners) {
        if (board[corner] === null) {
            return corner // A corner is available
        }
    }

    const edges = []
    for (let i = 1; i < sqrtOfBoard - 1; i++) {
        edges.push(i)
        edges.push(i * sqrtOfBoard)
        edges.push(i * sqrtOfBoard + sqrtOfBoard - 1)
        edges.push(board.length - sqrtOfBoard + i)
    }

    for (const edge of edges) {
        if (board[edge] === null) {
            return edge // An edge is available
        }
    }

    const emptyIndexes = board.reduce((acc, cell, index) => {
        if (cell === null) {
            acc.push(index as never)
        }
        return acc
    }, [])

    if (emptyIndexes.length > 0) {
        const randomIndex = Math.floor(Math.random() * emptyIndexes.length)
        return emptyIndexes[randomIndex]
    }

    return null
}