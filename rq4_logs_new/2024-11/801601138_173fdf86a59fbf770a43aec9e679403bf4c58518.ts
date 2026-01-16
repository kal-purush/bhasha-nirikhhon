import { ComboboxStrategy } from "../types";

export function useMultiComboboxStrategy<T>(
  selected: T[],
  setSelected: (newSelected: T[]) => void
): ComboboxStrategy<T> {
  const handleSelect = (newItem: T, getItemKey: (item: T) => string) => {
    const isAlreadySelected = selected.some(
      (item) => getItemKey(item) === getItemKey(newItem)
    );

    let updatedSelection;
    if (isAlreadySelected) {
      updatedSelection = selected.filter(
        (item) => getItemKey(item) !== getItemKey(newItem)
      );
    } else {
      updatedSelection = [...selected, newItem];
    }

    setSelected(updatedSelection);
    return updatedSelection;
  };

  return {
    handleSelect,
  };
}