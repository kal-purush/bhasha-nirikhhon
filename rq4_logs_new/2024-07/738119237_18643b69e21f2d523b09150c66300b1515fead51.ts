import { SelectionState } from 'morning-react-ui/types/dataTypes';

const selectionStateTrueToString = (
  selectionState: SelectionState | undefined,
): string => {
  if (selectionState === undefined) {
    return '';
  }

  const stringRepresentation = Object.entries(selectionState)
    .filter(([, value]) => value)
    .map(([key]) => key)
    .join(', ');

  return stringRepresentation.length > 0 ? `${stringRepresentation}, ` : '';
};

const selectionStateToArray = (selectionState: SelectionState): string[] => {
  return Object.entries(selectionState)
    .filter(([_, value]) => value === true)
    .map(([key]) => key);
};

const selectionStateToString = (
  selectionState: SelectionState | undefined,
): string | undefined => {
  if (!selectionState) {
    return undefined;
  }
  const trueKeys = Object.entries(selectionState)
    .filter(([, value]) => value === true)
    .map(([key]) => key);
  return trueKeys.length === 1 ? trueKeys[0] : undefined;
};

function getSelectedKeys(selectionState: SelectionState): string[] {
  return Object.keys(selectionState).filter((key) => selectionState[key]);
}

export {
  selectionStateTrueToString,
  selectionStateToArray,
  selectionStateToString,
  getSelectedKeys,
};