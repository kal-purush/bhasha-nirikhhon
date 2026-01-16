import { MoveInterface, updateGame } from "../../../api/firestore";
import { DocumentData } from "firebase/firestore";
import { stoneType } from "../Stones/Stone";

function getMoveNumber(updatedMoves: MoveInterface[]): number {
  if (updatedMoves.length > 0) {
    return updatedMoves[updatedMoves.length - 1].moveNumber + 1;
  } else {
    return 0;
  }
}

interface getUpdateMovesInterface {
  gameData: DocumentData;
  placedStoneId: string;
  placedStoneType: stoneType;
  loggedInUserUserId: string;
  movedFromColumnLetter: string;
  movedFromRowNumber: number;
  columnLetter: string;
  rowNumber: number;
}

export function getUpdatedMoves({
  gameData,
  placedStoneId,
  placedStoneType,
  loggedInUserUserId,
  movedFromColumnLetter,
  movedFromRowNumber,
  columnLetter,
  rowNumber,
}: getUpdateMovesInterface): MoveInterface[] {
  const movesToReturn = [...gameData.moves];
  movesToReturn.push({
    moveNumber: getMoveNumber(gameData.moves),
    id: placedStoneId,
    type: placedStoneType,
    movingPlayerId: loggedInUserUserId,
    fromCoordinates: `${movedFromColumnLetter}${movedFromRowNumber}`,
    targetCoordinates: `${columnLetter}${rowNumber}`,
    isTakeOver: false,
    isVictory: false,
  });

  return movesToReturn;
}

export function trackStoneMove(
  gameData: DocumentData,
  placedStoneId: string,
  placedStoneType: "CHICKEN" | "ELEPHANT" | "GIRAFFE" | "LION" | "HEN",
  loggedInUserUserId: string,
  movedFromColumnLetter: string,
  movedFromRowNumber: number,
  columnLetter: string,
  rowNumber: number
) {
  updateGame({
    id: gameData.gameId!,
    updatedDetails: {
      moves: getUpdatedMoves({
        gameData: gameData,
        placedStoneId,
        placedStoneType,
        loggedInUserUserId,
        movedFromColumnLetter,
        movedFromRowNumber,
        columnLetter,
        rowNumber,
      }),
    },
  });
}