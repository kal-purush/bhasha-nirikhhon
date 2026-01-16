import { buildThemeAliases } from "../../utils/buildThemeAliases";
import { colors } from "../colors";

const baseBorders = {
  none: "none",
  "1SolidGray": `1px solid ${colors.gray}`,
  "1SolidGrayLight": `1px solid ${colors.grayLight}`,
  "2SolidGrayLight": `2px solid ${colors.grayLight}`,
  "3SolidBlue": `3px solid ${colors.blue}`,
  "1SolidGreen": `1px solid ${colors.green}`,
  "1SolidMagenta": `1px solid ${colors.magenta}`,
  "3SolidMagenta": `3px solid ${colors.magenta}`,
  "1SolidNavy": `1px solid ${colors.navy}`,
  "1SolidWhite": `1px solid ${colors.white}`,
};

const baseAliases = buildThemeAliases(baseBorders, {
  error: "1SolidMagenta",
  menu: "1SolidGrayLight",
  success: "1SolidGreen",
});

export const base = {
  ...baseBorders,
  ...baseAliases,
};

export type Base = Record<keyof typeof base, string>;