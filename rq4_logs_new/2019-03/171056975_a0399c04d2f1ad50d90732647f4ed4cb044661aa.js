import Scenario from "../Scenario";
import {squareNameToArrayIndices} from "../../helpers";

// TODO remove dependency from MovePieceScenario on Scenario and replace with mock

const INITIAL_BOARD = [
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null],
    [null, null, null, null, null, null, null, null]
];

export default class TestScenario extends Scenario {
    constructor(enPassantSquareName=undefined) {
        super(INITIAL_BOARD, enPassantSquareName);
    }

    __setPiece(algebraicNotation, type, color, hasMoved=true) {
        const [x, y] = squareNameToArrayIndices(algebraicNotation);
        this.board[x][y] = {type, color, hasMoved};
    }
}