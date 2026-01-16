/** @jsx jsx */
import { jsx } from "@emotion/core";
import React, { useState } from "react";
import ReactDOM from "react-dom";
import { FlexRow, FlexColumn } from "./helpers";
import { SquareButton, Grow, Row } from "./helpers";

import styled from "@emotion/styled";
import { theme, styles } from "./theme";

export const ToggleButton = styled.div(props => ({
  fontWeight: 500,
  width: props.square ? 18 : props.style.width,
  height: props.square ? 18 : props.style.width,
  backgroundColor: props.set ? "black" : "transparent"
}));

export const Button = styled.div(props => ({
  fontWeight: 600,
  cursor: "default",
  justifyContent: "center",
  display: props.hidden ? "none" : "flex",
  flexGrow: 0,
  flexShrink: 0,
  border: "none",
  boxSixing: "border-box",
  flexDirection: "row",
  alignItems: "center",
  outline: "none",
  borderRadius: 3,
  padding: 0,
  minHeight: 18,
  minWidth: 18,
  transition: props.immediate ? "none" : "all .1s ease-in-out",
  opacity: props.disabled ? 0.5 : 1,
  pointerEvents: props.disabled ? "none" : "initial",
  boxShadow: props.outlined ? "inset 0px 0px 0px 1px hsl(0,0%,90%)" : undefined,
  color: props.primary ? "white" : "inherit",
  backgroundColor: props.transparent
    ? "transparent"
    : props.primary
    ? "hsl(211,100%,50%)"
    : "hsl(0,0%,90%)",
  "&:hover": {
    backgroundColor: props.primary
      ? "hsl(211,100%,45%)"
      : props.transparent
      ? "transparent"
      : "hsl(0,0%,85%)",
    boxShadow: props.outlined
      ? "inset 0px 0px 0px 1px  hsl(0,0%,80%)"
      : undefined,
    filter: props.silent || props.outlined ? undefined : "brightness(90%)"
  },
  "&:active": {
    outline: "none",
    border: "none",
    boxShadow: props.outlined
      ? "inset 0px 0px 0px 1px  hsl(0,0%,80%)"
      : undefined,
    backgroundColor: props.transparent
      ? "transparent"
      : props.primary
      ? "hsl(211,100%,40%)"
      : "hsl(0,0%,80%)",
    transform: props.silent ? "none" : "translateY(1px)"
  }
}));