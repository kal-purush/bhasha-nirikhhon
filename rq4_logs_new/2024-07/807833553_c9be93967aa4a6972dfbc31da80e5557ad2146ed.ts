import { ArgTypes } from "@storybook/react";
import { BaseTypographyProps } from "./types";

export const argTypes: Partial<ArgTypes<BaseTypographyProps>> = {
  textTransform: {
    control: {
      type: "select",
    },
    options: ["none", "uppercase", "lowercase", "capitalize"],
  },
  textAlign: {
    control: {
      type: "select",
    },
    options: ["left", "center", "right"],
  },
  textWrap: {
    control: {
      type: "select",
    },
    options: ["nowrap", "wrap"],
  },
};