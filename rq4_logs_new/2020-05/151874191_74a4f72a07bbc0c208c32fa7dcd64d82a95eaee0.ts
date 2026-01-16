import { Cell, GameState } from './shared';

export const getChangedStateMock = (
  state: GameState,
  cell: Cell,
  cells: Cell[] = []
): GameState => ({
  ...state,
  turn: {
    ...state.turn,
    cells: state.turn.cells.map(
      (initialCell) => cells.find(({ id }) => id === initialCell.id) || initialCell
    ),
    cell,
  }
});