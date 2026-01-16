import { vars } from "./vars.css";
import { defineProperties, createSprinkles } from "@vanilla-extract/sprinkles";

const responsiveProperties = defineProperties({
  properties: {
    padding: vars.space,
    display: ["none", "flex", "block", "inline", "grid"],
    flexDirection: ["row", "column"],
    justifyContent: [
      "stretch",
      "flex-start",
      "center",
      "flex-end",
      "space-around",
      "space-between",
    ],
    textAlign: ["center", "left", "right"],
    alignItems: ["stretch", "flex-start", "center", "flex-end"],
    paddingTop: vars.space,
    paddingBottom: vars.space,
    paddingLeft: vars.space,
    paddingRight: vars.space,
    marginTop: vars.space,
    marginRight: vars.space,
    marginLeft: vars.space,
    marginBottom: vars.space,
    fontFamily: vars.fontFamily,
    fontSize: vars.fontSize,
  },
  shorthands: {
    padding: ["paddingTop", "paddingBottom", "paddingLeft", "paddingRight"],
    paddingX: ["paddingLeft", "paddingRight"],
    paddingY: ["paddingTop", "paddingBottom"],
    placeItems: ["justifyContent", "alignItems"],
  },
  conditions: {
    mobile: {},
    tablet: { "@media": "screen and (min-width: 768px)" },
    desktop: { "@media": "screen and (min-width: 1024px)" },
  },
  defaultCondition: "mobile",
});

const colorProperties = defineProperties({
  properties: {
    color: vars.color,
    background: vars.color
  }
});

export const sprinkles = createSprinkles(
  responsiveProperties,
  colorProperties
);

export type Sprinkles = Parameters<typeof sprinkles>[0];