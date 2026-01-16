import * as CSS from "csstype";
import { system } from "styled-system";

export const listStyle = system({
  listStyle: true,
});

export interface ListStyleProps {
  listStyle?: CSS.Property.ListStyleType;
}