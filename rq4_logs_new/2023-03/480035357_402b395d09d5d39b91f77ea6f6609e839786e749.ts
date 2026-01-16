import { Type } from "./types";

export default {
  name: "DProgress",
  className: "dProgress",
  role: "progressbar",
  labelClassName: "label",
  progressClassName: "progress",
  linearClassName: "linear",
  circularClassName: "circular",
  contentClassName: "content",
  loaderContainerClassName: "loaderContainer",
  loaderClassName: "loader",
  captionClassName: "caption",
  labelOffsetCSSPropName: "--offset", // TODO: share with css ...
  defaultLabelOffset: "2px", // TODO: share with css ...
  captionOffsetCSSPropName: "--offset", // TODO: share with css ...
  defaultCaptionOffset: "2px", // TODO: share with css ...
  defaultType: Type.linear,
} as const;