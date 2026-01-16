import { KeyCode } from "store/keyboard/models";
import { KeyConfig } from "./models";

export const keyboardConfig: KeyConfig[][] = [
  [
    {
      mainTitle: "`",
      subTitle: "~",
      code: KeyCode.Backquote,
    },
    {
      mainTitle: "1",
      subTitle: "!",
      code: KeyCode.DigitOne,
    },
    {
      mainTitle: "2",
      subTitle: "@",
      code: KeyCode.DigitTwo,
    },
    {
      mainTitle: "3",
      subTitle: "#",
      code: KeyCode.DigitThree,
    },
    {
      mainTitle: "4",
      subTitle: "$",
      code: KeyCode.DigitFour,
    },
    {
      mainTitle: "5",
      subTitle: "%",
      code: KeyCode.DigitFive,
    },
    {
      mainTitle: "6",
      subTitle: "^",
      code: KeyCode.DigitSix,
    },
    {
      mainTitle: "7",
      subTitle: "&",
      code: KeyCode.DigitSeven,
    },
    {
      mainTitle: "8",
      subTitle: "*",
      code: KeyCode.DigitEight,
    },
    {
      mainTitle: "9",
      subTitle: "(",
      code: KeyCode.DigitNine,
    },
    {
      mainTitle: "0",
      subTitle: ")",
      code: KeyCode.DigitZero,
    },
    {
      mainTitle: "-",
      subTitle: "_",
      code: KeyCode.Minus,
    },
    {
      mainTitle: "=",
      subTitle: "+",
      code: KeyCode.Equal,
    },
    {
      mainTitle: "Backspace",
      code: KeyCode.Backspace,
    },
  ],
  [
    {
      mainTitle: "Tab",
      code: KeyCode.Tab,
    },
    {
      mainTitle: "Q",
      code: KeyCode.KeyQ,
    },
    {
      mainTitle: "W",
      code: KeyCode.KeyW,
    },
    {
      mainTitle: "E",
      code: KeyCode.KeyE,
    },
    {
      mainTitle: "R",
      code: KeyCode.KeyR,
    },
    {
      mainTitle: "T",
      code: KeyCode.KeyT,
    },
    {
      mainTitle: "Y",
      code: KeyCode.KeyY,
    },
    {
      mainTitle: "U",
      code: KeyCode.KeyU,
    },
    {
      mainTitle: "I",
      code: KeyCode.KeyI,
    },
    {
      mainTitle: "O",
      code: KeyCode.KeyO,
    },
    {
      mainTitle: "P",
      code: KeyCode.KeyP,
    },
    {
      mainTitle: "[",
      subTitle: "{",
      code: KeyCode.BracketLeft,
    },
    {
      mainTitle: "]",
      subTitle: "}",
      code: KeyCode.BracketRight,
    },
    {
      mainTitle: "\\",
      subTitle: "|",
      code: KeyCode.Backslash,
    },
  ],
  [
    {
      mainTitle: "Caps Lock",
      code: KeyCode.CapsLock,
    },
    {
      mainTitle: "A",
      code: KeyCode.KeyA,
    },
    {
      mainTitle: "S",
      code: KeyCode.KeyS,
    },
    {
      mainTitle: "D",
      code: KeyCode.KeyD,
    },
    {
      mainTitle: "F",
      code: KeyCode.KeyF,
    },
    {
      mainTitle: "G",
      code: KeyCode.KeyG,
    },
    {
      mainTitle: "H",
      code: KeyCode.KeyH,
    },
    {
      mainTitle: "J",
      code: KeyCode.KeyJ,
    },
    {
      mainTitle: "K",
      code: KeyCode.KeyK,
    },
    {
      mainTitle: "L",
      code: KeyCode.KeyL,
    },
    {
      mainTitle: ";",
      subTitle: ":",
      code: KeyCode.Semicolon,
    },
    {
      mainTitle: "'",
      subTitle: '"',
      code: KeyCode.Quote,
    },
    {
      mainTitle: "Enter",
      code: KeyCode.Enter,
    },
  ],
  [
    {
      mainTitle: "Shift",
      code: KeyCode.ShiftLeft,
    },
    {
      mainTitle: "Z",
      code: KeyCode.KeyZ,
    },
    {
      mainTitle: "X",
      code: KeyCode.KeyX,
    },
    {
      mainTitle: "C",
      code: KeyCode.KeyC,
    },
    {
      mainTitle: "V",
      code: KeyCode.KeyV,
    },
    {
      mainTitle: "B",
      code: KeyCode.KeyB,
    },
    {
      mainTitle: "N",
      code: KeyCode.KeyN,
    },
    {
      mainTitle: "M",
      code: KeyCode.KeyM,
    },
    {
      mainTitle: ",",
      subTitle: "<",
      code: KeyCode.Comma,
    },
    {
      mainTitle: ".",
      subTitle: ">",
      code: KeyCode.Period,
    },
    {
      mainTitle: "/",
      subTitle: "?",
      code: KeyCode.Slash,
    },
    {
      mainTitle: "Shift",
      code: KeyCode.ShiftRight,
    },
  ],
  [
    {
      mainTitle: "Ctrl",
      code: KeyCode.MetaLeft,
    },
    {
      mainTitle: "Win",
      code: KeyCode.ControlLeft,
    },
    {
      mainTitle: "Alt",
      code: KeyCode.AltLeft,
    },
    {
      mainTitle: " ",
      code: KeyCode.Space,
    },
    {
      mainTitle: "Alt",
      code: KeyCode.AltRight,
    },
    {
      mainTitle: "Win",
      code: KeyCode.ControlRight,
    },
    {
      mainTitle: "Menu",
      code: KeyCode.ContextMenu,
    },
    {
      mainTitle: "Ctrl",
      code: KeyCode.MetaRight,
    },
  ],
];

export const shiftCombinationChars: string[] = [
  "~",
  "@",
  "#",
  "$",
  "%",
  "^",
  "&",
  "*",
  "(",
  ")",
  "-",
  "+",
  ":",
  '"',
  "<",
  ">",
  "?",
];