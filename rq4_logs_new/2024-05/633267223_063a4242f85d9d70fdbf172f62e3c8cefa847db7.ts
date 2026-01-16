import { useMemo, useState } from 'react';
import { DIRECTIONS, INITIAL_BOARD } from '../constants/defaultValues';

const canPut = (board: number[][], x: number, y: number, turnColor: number) =>
  DIRECTIONS.map(
    ([dx, dy]) =>
      [...Array(8).keys()]
        .map((i) => board[y + dy * i]?.[x + dx * i])
        .filter((v) => v !== undefined)
        .map((v) => Math.max(v, 0))
        .with(0, turnColor)
        .join('')
        .match(/^((12+1)|(21+2))/) !== null,
  ).some(Boolean);

const canPutLength = (
  board: number[][],
  x: number,
  y: number,
  turnColor: number,
  [dx, dy]: [number, number],
) => {
  return [...Array(8).keys()]
    .map((i) => board[y + dy * i]?.[x + dx * i])
    .filter((v) => v !== undefined)
    .map((v) => Math.max(v, 0))
    .with(0, turnColor)
    .join('')
    .match(/^((12+1)|(21+2))/)?.[0]?.length;
};

const searchCanPut = (board: number[][], turnColor: number) =>
  board.map((row, y) =>
    row.map(
      (_, x) =>
        [-canPut(board, x, y, turnColor), board[y][x]][Math.max(Math.min(board[y][x], 1), 0)],
    ),
  );

export const useBoard = () => {
  const [turnColor, setTurnColor] = useState(1);
  const [board, setBoard] = useState(INITIAL_BOARD);
  const [whiteCount, blackCount] = useMemo(
    () => [1, 2].map((color) => board.flat().filter((v) => v === color).length),
    [board],
  );
  const checkedBoard = useMemo(() => searchCanPut(board, turnColor), [board, turnColor]);
  const isGameEnd = useMemo(
    () =>
      [1, 2].every(
        (color) =>
          searchCanPut(board, color)
            .flat()
            .filter((cell) => cell === -1).length === 0,
      ),
    [board],
  );

  const reverseStone = (board: number[][], x: number, y: number): number[][] => {
    const newBoard = structuredClone(board);
    const replacePositions = DIRECTIONS.flatMap(([dx, dy]) => {
      const length = canPutLength(board, x, y, turnColor, [dx, dy]);
      const lengthArray = Array(+Number.isFinite(length) * [...Array(length)].length);
      return [...lengthArray.keys()].map((i) => ({ x: x + dx * i, y: y + dy * i }));
    });

    const replacedBoard = replacePositions.reduce((acc, { x, y }) => {
      acc[y][x] = turnColor;
      return acc;
    }, newBoard);

    return replacedBoard;
  };

  const handleCellClick = (x: number, y: number) => {
    const action = [
      () => {},
      () => {
        const reversedBoard = reverseStone(board, x, y);
        const shouldPass = searchCanPut(reversedBoard, turnColor ^ 3)
          .flat()
          .every((cell) => cell !== -1);
        setBoard(reversedBoard);
        setTurnColor((turnColor ^ 3) * +!shouldPass + turnColor * +shouldPass);
      },
    ];

    action[-checkedBoard[y][x]]?.();
  };

  const finishGame = () => {
    [
      () => {},
      () => {
        const message = ['白の勝ち', '黒の勝ち', '引き分け'][
          +(blackCount >= whiteCount) + +(blackCount === whiteCount)
        ];
        setTimeout(() => alert(`${message}です`), 0);
      },
    ][+isGameEnd]?.();
  };

  finishGame();

  return {
    board: checkedBoard,
    game: {
      turnColor,
      count: {
        white: whiteCount,
        black: blackCount,
      },
    },
    handleCellClick,
  };
};