import { Injectable } from "src/withXeno/inversify";

@Injectable()
export class ElmPositionService {
  getElmPosition = (
    elmId: string
  ): { top: number; left: number; h: number; w: number } => {
    const elm = document.getElementById(elmId);
    if (!elm) {
      return {
        top: 0,
        left: 0,
        h: 0,
        w: 0,
      };
    }

    const { offsetLeft, offsetHeight, offsetTop, offsetWidth } = elm;
    return {
      top: offsetTop,
      left: offsetLeft,
      h: offsetHeight,
      w: offsetWidth,
    };
  };
}