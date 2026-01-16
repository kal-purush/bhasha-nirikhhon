export type CalculateOffset = (coord: number, offset: number, oldZoom: number, newZoom: number, size: number) => number;

// Shim type
interface IWebAssemblyModule {
  instance: {
    exports: IExports;
  };
}

interface IExports {
  calculateOffset: CalculateOffset;
}

// Shim Type
declare var WebAssembly: any;

export class WasmUtil {

  public static get calculateOffset(): CalculateOffset {
    return WasmUtil.importObject.calculateOffset;
  }

  /**
   * Attempts to load the wasm into the browser
   */
  public static initialize(): Promise<void> {
    return fetch("assets/rust-utils.wasm").then((response) => {
      return response.arrayBuffer();
    }).then((bytes) => {
      return WebAssembly.instantiate(bytes, WasmUtil.importObject);
    }).then((object: IWebAssemblyModule) => {
      WasmUtil.importObject = object.instance.exports;
      WasmUtil.isInit = true;
    }).catch(() => {
      WasmUtil.importObject = {
        calculateOffset,
      };
    });
  }

  public static get initialized() {
    return WasmUtil.isInit;
  }

  private static importObject: IExports;

  private static isInit = false;

  private constructor() {
    // This class is a singleton
  }
}

/**
 * Provided as a failback in case the wasm for the method is unable to be loaded
 * @param coord Coord to calculate new offset from
 * @param offset Old offset
 * @param oldZoom Old zoom of the canvas
 * @param newZoom New zoom of the canvas
 * @param size Size for the coord to range within
 */
function calculateOffset(coord: number, offset: number, oldZoom: number, newZoom: number, size: number) {
  const newValue = size / newZoom;

  const gap = size - newValue;
  let firstSide = offset;
  if (firstSide < 0) {
    firstSide = 0;
  } else if (firstSide > gap) {
    firstSide = gap;
  }

  let secondSide = offset + size / oldZoom - newValue;
  if (secondSide > gap) {
    secondSide = gap;
  } else if (secondSide < 0) {
    secondSide = 0;
  }

  const boundary = [firstSide, secondSide];
  let newOffset = coord - newValue / 2;
  if (newOffset < boundary[0]) {
    newOffset = boundary[0];
  } else if (newOffset > boundary[1]) {
    newOffset = boundary[1];
  }
  return newOffset;
}