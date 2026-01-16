import { EntityType } from '../records/EntityRecord';
import { CommandType } from '../records/CommandRecord';
import { ExitType } from '../records/ExitRecord';

export const COMMAND_SELECT_AUTOCOMPLETE_ITEM = 'COMMAND_SELECT_AUTOCOMPLETE_ITEM';
export const COMMAND_CLOSE_AUTOCOMPLETE = 'COMMAND_CLOSE_AUTOCOMPLETE';
export const COMMAND_COMPLETE = 'COMMAND_COMPLETE';
export const COMMAND_HISTORY_CLEAR = 'COMMAND_HISTORY_CLEAR';
export const COMMAND_SEND = 'COMMAND_SEND';
export const COMMAND_SET_CURSOR_INDEX = 'COMMAND_SET_CURSOR_INDEX';
export const COMMAND_SET_CURRENT = 'COMMAND_SET_CURRENT';
export const EDITOR_ADD_VIEW = 'EDITOR_ADD_VIEW';
export const EDITOR_HISTORY_CLEAR = 'EDITOR_HISTORY_CLEAR';
export const EDITOR_REMOVE_VIEW = 'EDITOR_REMOVE_VIEW';
export const EDITOR_SELECT_ITEMS = 'EDITOR_SELECT_ITEMS';
export const EDITOR_SET_ACTIVE_VIEW = 'EDITOR_SET_ACTIVE_VIEW';
export const INVENTORY_EXPAND_ITEMS = 'INVENTORY_EXPAND_ITEMS';
export const INVENTORY_TOGGLE_EXPAND = 'INVENTORY_TOGGLE_EXPAND';
export const INVENTORY_TOGGLE_SELECT = 'INVENTORY_TOGGLE_SELECT';
export const PLAYER_SET_ACTIVE_VIEW = 'CHARACTER_SET_ACTIVE_VIEW';
export const RESIZE_PANEL = 'RESIZE_PANEL';
export const SET_STATE = 'SET_STATE';
export const SOCKET_STATUS = 'SOCKET_STATUS';

export type AutocompleteOption = EntityType | CommandType | ExitType;

export interface Action {
  type: string;
  payload?: Object;
}