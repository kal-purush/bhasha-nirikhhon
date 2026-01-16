import { style } from "@vanilla-extract/css";
import { sprinkles } from "../../styles/sprinkles.css";


export const button = style([
  sprinkles({
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    background: "primary",
    color: "white",
    padding: "large",
  }),
  {
    fontWeight: 700,
    width: "100%",
    marginTop: "1.8rem",
    outline: "none",
    border: "none",
    cursor: "pointer",
    transitionDuration: "200ms",

    ':hover': {
      background: '#B55147',
    },

    ':active': {
      background: '#883D35',
    },

    ':disabled': {
      opacity: 0.4,
    }
  }
])