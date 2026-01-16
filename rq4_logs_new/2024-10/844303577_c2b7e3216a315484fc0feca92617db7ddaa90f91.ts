import type { CSSProperties } from "react";
import type { PopoverOrigin } from "@mui/material";

export type TPopoverOrigin = {
  transform: PopoverOrigin;
  anchor: PopoverOrigin;
};

export type TArrowPosition =
  | "left-top"
  | "left-center"
  | "left-bottom"
  | "right-top"
  | "right-center"
  | "right-bottom"
  | "top-left"
  | "top-center"
  | "top-right"
  | "bottom-left"
  | "bottom-center"
  | "bottom-right";

const PopoverUtils = {
  calculateArrowPosition: {
    "left-top": {
      left: 0,
      top: 15,
      transform: "translateX(-50%) rotate(45deg)",
    },
    "left-center": {
      left: 0,
      bottom: "50%",
      transform: "translate(-50%, 50%) rotate(45deg)",
    },
    "left-bottom": {
      left: 0,
      bottom: 15,
      transform: "translateX(-50%) rotate(45deg)",
    },
    "right-top": {
      right: 0,
      top: 15,
      transform: "translateX(50%) rotate(45deg)",
    },
    "right-center": {
      right: 0,
      bottom: "50%",
      transform: "translate(50%, 50%) rotate(45deg)",
    },
    "right-bottom": {
      right: 0,
      bottom: 15,
      transform: "translateX(50%) rotate(45deg)",
    },
    "top-left": {
      top: 0,
      left: 15,
      transform: "translateY(-50%) rotate(45deg)",
    },
    "top-center": {
      top: 0,
      left: "50%",
      transform: "translate(-50%, -50%) rotate(45deg)",
    },
    "top-right": {
      top: 0,
      right: 15,
      transform: "translateY(-50%) rotate(45deg)",
    },
    "bottom-left": {
      bottom: 0,
      left: 15,
      transform: "translateY(50%) rotate(45deg)",
    },
    "bottom-center": {
      bottom: 0,
      left: "50%",
      transform: "translate(-50%, 50%) rotate(45deg)",
    },
    "bottom-right": {
      bottom: 0,
      right: 15,
      transform: "translateY(50%) rotate(45deg)",
    },
  } as Record<TArrowPosition, CSSProperties>,
  getGutterPosition: {
    "left-top": "marginLeft",
    "left-center": "marginLeft",
    "left-bottom": "marginLeft",
    "right-top": "marginRight",
    "right-center": "marginRight",
    "right-bottom": "marginRight",
    "top-left": "marginTop",
    "top-center": "marginTop",
    "top-right": "marginTop",
    "bottom-left": "marginBottom",
    "bottom-center": "marginBottom",
    "bottom-right": "marginBottom",
    none: "margin",
  } as Record<TArrowPosition | "none", "marginLeft" | "marginRight" | "marginTop" | "marginBottom" | "margin">,
};

export default PopoverUtils;