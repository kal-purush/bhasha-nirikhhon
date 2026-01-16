import { SelectAdapter } from "@/shared/libs";
import { ReactNode } from "react";

// Base properties shared between single and multi comboboxes
interface BaseComboboxProps<T> extends SelectAdapter<T> {
  renderInList?: (value: T) => ReactNode;
  renderSelected?: (value: T) => ReactNode;
  selectPlaceholder?: string;
  searchPlaceholder?: string;
  noItemsText?: string;
  buttonClassName?: string;
  contentClassName?: string;
  searchDelay?: number;
}

// Single Combobox
export type ComboboxHandle<T> = {
  getSelected: () => T | undefined;
  setSelected: (newSelected?: T) => any;
};

export interface ComboboxProps<T> extends BaseComboboxProps<T> {
  value?: T;
  onSelect?: (value?: T) => any;
}

// Multi Combobox Handle and Props
export type MultiComboboxHandle<T> = {
  setSelected: (newSelected: T[]) => void;
};

export interface MultiComboboxProps<T> extends BaseComboboxProps<T> {
  onSelect?: (value?: T[]) => any;
  multi?: boolean;
}