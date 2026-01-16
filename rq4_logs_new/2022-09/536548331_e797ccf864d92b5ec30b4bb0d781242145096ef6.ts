import { AppState } from "../store/index";
import { VisibilityFilterTypes } from "../types/state/visibilityFilter";

export const visibilityFilter = (state: AppState): VisibilityFilterTypes =>
  state.visibilityFilter;