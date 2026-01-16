type TicTacToePiece = 'X' | 'O' | null;
type ColIndex = 0 | 1 | 2;
type BoardCol = [TicTacToePiece, TicTacToePiece, TicTacToePiece];
export type Player = -1 | 1;
export type Cell = [ColIndex, ColIndex];
export type Board = [
    [TicTacToePiece, TicTacToePiece, TicTacToePiece],
    [TicTacToePiece, TicTacToePiece, TicTacToePiece],
    [TicTacToePiece, TicTacToePiece, TicTacToePiece]
];

const isColIndex = (num: Number): num is ColIndex => num > 0;

export const aiMove = ({
    board,
    player,
}: {
    board: Board;
    player: Player;
}): Board => {
    const newBoard = board;
    const blocks = mustBlock({board, player});
    if (blocks.length > 0) {
        newBoard[blocks[0][0]][blocks[0][1]] = player === 1 ? 'O' : 'X';
        return newBoard;
    }
    else {
        let index = -1;
        for (let i = 0; i < 3; i++) {
            for (let j = 0; j < 3; j++) {
                if (newBoard[i][j] === null) {
                    newBoard[i][j] = player === 1 ? 'O' : 'X';
                    return newBoard;
                }
            }
        }
    }
};
export const playerMove = ({
    board, 
    player, 
    cell,
}: {
    board: Board, 
    player: Player, 
    cell: Cell
}): Promise<Board> => new Promise((res, rej) => {
    if (board[cell[0]][cell[1]] === null) {
        const newBoard = board;
        newBoard[cell[0]][cell[1]] = player === 1 ? 'X' : 'O';
        res(newBoard);
    }
});

const mustBlock = ({
    board,
    player,
}: {
    board: Board;
    player: Player;
}): Cell[] => {
    const mustBlockArr = (
        arr: BoardCol,
    ): ColIndex | false => {
        if (
            arr.filter((piece) => piece === (player === 1 ? 'O' : 'X'))
                .length === 2 &&
            !arr.find((piece) => piece === (player === 1 ? 'X' : 'O'))
        ) {
            const i = arr.indexOf(null);
            return isColIndex(i) ? i : false;
        }
        return false;
    };
    const rowIndices = board.reduce((res, row, i) => {
        const j = mustBlockArr(row);
        if (typeof j === "number") {
            return [...res, [i, j]];
        }
        return res;
    }, []);
    //implement for cols and diags
    return rowIndices;
};