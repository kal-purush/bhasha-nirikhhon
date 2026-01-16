import { ICreateOptions, ISetupOptions, Keys } from './interfaces/uinput';

export type allowedInputs =
  | 'a'
  | 'b'
  | 'x'
  | 'y'
  | 'start'
  | 'select'
  | 'l1'
  | 'l2'
  | 'r1'
  | 'r2'
  | 'up'
  | 'down'
  | 'left'
  | 'right'
  | 'admin_end_game'
  | 'admin_save_game'
  | 'admin_load_game';

export function getCommandRegex(): RegExp {
  return /^(a|b|x|y|start|select|l1|l2|r1|r2|up|down|left|right|admin_end_game|admin_save_game|admin_load_game)$/;
}

export function getSetupOptions(): ISetupOptions {
  return { EV_KEY: [] };
}

export function getCreateOptions(uinput: any): ICreateOptions {
  return {
    name: 'myuinput',
    id: {
      bustype: uinput.BUS_VIRTUAL,
      vendor: 0x1,
      product: 0x1,
      version: 1
    }
  };
}

const keyMap = {
  a: Keys.KEY_A,
  b: Keys.KEY_B,
  x: Keys.KEY_X,
  y: Keys.KEY_Y,
  start: Keys.KEY_S,
  select: Keys.KEY_E,
  l1: Keys.KEY_1,
  l2: Keys.KEY_2,
  r1: Keys.KEY_3,
  r2: Keys.KEY_4,
  up: Keys.KEY_UP,
  down: Keys.KEY_DOWN,
  left: Keys.KEY_LEFT,
  right: Keys.KEY_RIGHT,
  admin_end_game: Keys.KEY_END, // TODO Combo
  admin_save_game: Keys.KEY_SAVE,
  admin_load_game: Keys.KEY_REFRESH
};

export function toKey(ircInput: allowedInputs): Keys {
  return keyMap[ircInput];
}