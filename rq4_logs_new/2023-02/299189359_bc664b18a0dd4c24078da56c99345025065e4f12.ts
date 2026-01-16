import React, { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import type { Board, Store, Square } from '~/types';
import { changeGamesTurn, updateScore } from '~/redux/modules/game';
import Utils from '~/utils';
import { PieceColor, SizeMap } from '~/const';

const {
  Game: { getUpdatableSquaresArray },
} = Utils;

const useBoardSelector = (): {
  squareList: Board;
  boardSize: number;
} => {
  const { sideSquaresCount, board: squareList } = useSelector(
    (store: Store) => store.game,
  );

  const boardSize = sideSquaresCount * SizeMap.SQUARE_SIZE;

  return {
    squareList,
    boardSize,
  };
};

const useBoardScroll = (
  ref: React.RefObject<HTMLDivElement>,
  boardSize: number,
): void => {
  useEffect(() => {
    if (ref.current) {
      const refCurrent = ref.current;
      const rect = refCurrent.getBoundingClientRect();
      refCurrent.scrollTop = boardSize / 2 - rect.height / 2;
      refCurrent.scrollLeft = boardSize / 2 - rect.width / 2;
    }
  }, [ref, boardSize]);
};

const useBoardFunctions = (): {
  hasCanBeTurnOverPieces: (square: Square) => boolean;
  hasPlacedPiece: (square: Square) => boolean;
  placePiece: (square: Square) => void;
} => {
  const dispatch = useDispatch();

  // check if there is a stone that can be turned over
  const hasCanBeTurnOverPieces = (square: Square): boolean => {
    const updatableSquaresArray: Square[] = getUpdatableSquaresArray(square);

    return (
      !!(square.pieceColor === PieceColor.INVISIBLE) &&
      !!(updatableSquaresArray.length > 0)
    );
  };

  // check if a stone has already been placed
  const hasPlacedPiece = (square: Square): boolean =>
    !!(square.pieceColor !== PieceColor.INVISIBLE);

  // turn over the stone that was trapped when the stone was placed and switch the current player
  const placePiece = (clickedSquare: Square) => {
    const updatableSquaresArray: Square[] =
      getUpdatableSquaresArray(clickedSquare);

    dispatch(changeGamesTurn(clickedSquare, updatableSquaresArray));
    dispatch(updateScore());
  };

  return {
    hasCanBeTurnOverPieces,
    hasPlacedPiece,
    placePiece,
  };
};

export { useBoardSelector, useBoardScroll, useBoardFunctions };