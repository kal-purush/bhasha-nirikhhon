import {CountdownLevel} from "ctx/Countdown";
import {WinSize} from "ctx/WinSize";

export interface IDefLevWin {
  lev: CountdownLevel;
  win: WinSize;
  img: string;
}

interface IDefLev {
  lev: CountdownLevel;
  img: string;
}

interface IDefWin {
  win: WinSize,
  img: string;
}


interface IKitDefWin {
  lev: CountdownLevel;
  win: IDefWin[];
}

interface IKitWin {
  lev: CountdownLevel;
  win: WinSize[];
  img: string;
}

interface IWinLevKit {
  win: WinSize,
  lev: IDefLev[]
}

export type IProgressiveImgParam =
  IDefLevWin
  | IKitDefWin
  | IDefLev
  | IWinLevKit
  | IKitWin;
