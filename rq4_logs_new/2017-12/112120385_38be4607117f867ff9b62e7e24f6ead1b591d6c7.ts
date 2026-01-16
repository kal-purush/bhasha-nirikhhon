import { AnyAction } from "redux";
import { CanvasActions } from "./pixel-canvas.actions";

export interface IPixelCanvasData {
  color: string;
  height: number;
  pixels: string[][];
  width: number;
}

const SOLID_WHITE = "#FFFFFF";

export function getInitialState(): IPixelCanvasData {
  const pixelArray: string[][] = [];
  pixelArray.length = 6;
  for (let i = 0; i < 6; i++) {
    pixelArray[i] = [SOLID_WHITE, SOLID_WHITE, SOLID_WHITE, SOLID_WHITE];
  }

  return {
    color: "#CCCCCC",
    height: 4,
    pixels: pixelArray,
    width: 6,
  };
}

export function dataReducer(lastState: IPixelCanvasData = getInitialState(), action: AnyAction) {
  let newState: IPixelCanvasData;

  switch (action.type) {
    case CanvasActions.CANVAS_CLICK:
      const x = action.value[0];
      const y = action.value[1];

      if (lastState.pixels[x][y] === lastState.color) {
        newState = lastState;
      } else {
        newState = Object.assign({}, lastState);
        newState.pixels = newState.pixels.slice();
        newState.pixels[x] = newState.pixels[x].slice();
        newState.pixels[x][y] = newState.color;
      }

      return newState;
    case CanvasActions.CHANGE_COLOR:
      newState = Object.assign({}, lastState);
      newState.color = action.value;
      return newState;
    case CanvasActions.CHANGE_HEIGHT:
      if (!(action.value > 1)) {
        return Object.assign({}, lastState);
      }
      return changeHeight(lastState, action.value);
    case CanvasActions.CHANGE_WIDTH:
      if (!(action.value > 1)) {
        return Object.assign({}, lastState);
      }
      return changeWidth(lastState, action.value);
    case CanvasActions.CLEAR_CANVAS:
      return getInitialState();
    default:
      return lastState;
  }
}

/**
 * Takes in the state of the canvas and returns a new canvas state with the given height.
 * All new cells are colored solid white.
 * @param state Current state object
 * @param newHeight New height to use for the canvas
 */
function changeHeight(state: IPixelCanvasData, newHeight: number): IPixelCanvasData {
  const newState = Object.assign({}, state);
  const lastHeight = state.height;

  newState.pixels = newState.pixels.map((column) => {
    column = column.slice();
    column.length = newHeight;
    for (let i = lastHeight; i < newHeight; i++) {
      column[i] = SOLID_WHITE;
    }
    return column;
  });

  newState.height = newHeight;
  return newState;
}

/**
 * Takes in the state of the canvas and returns a new canvas state with the given width.
 * All new cells are colored solid white.
 * @param state Current state object
 * @param newWidth New width to use for the canvas
 */
function changeWidth(state: IPixelCanvasData, newWidth: number): IPixelCanvasData {
  const newState = Object.assign({}, state);
  const lastWidth = state.width;

  const newColumn: string[] = [];
  for (let i = 0; i < newState.height; i++) {
    newColumn.push(SOLID_WHITE);
  }

  newState.pixels = newState.pixels.slice();
  newState.pixels.length = newWidth;
  for (let i = lastWidth; i < newWidth; i++) {
    newState.pixels[i] = newColumn.slice();
  }

  newState.width = newWidth;
  return newState;
}