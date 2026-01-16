export interface IGlobalState {
  adminModeEnabled: boolean;
}

export interface IGlobalStateShape {
  state: IGlobalState;
  setPartialState: ISetPartialState;
  isAdmin: boolean;
}

export type ISetPartialState = (partialState: Partial<IGlobalState>) => void;

export const DEFAULT_STATE: IGlobalState = {
  adminModeEnabled: false,
};