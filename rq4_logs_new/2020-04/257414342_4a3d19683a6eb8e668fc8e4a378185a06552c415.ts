import { createStyles, makeStyles } from "@material-ui/core/styles";
import palette from "../../../theme/palette";

export const PokerStyles = makeStyles(() =>
  createStyles({
    cards: {
      marginTop: "1.5rem",
      position: "relative",
      background: "transparent",
      padding: "1.5rem 0",
      display: "flex",
      flexFlow: "row nowrap",
    },

    card: {
      background: palette.primary,
      color: palette.veryDarkGray,
      fontSize: "2.8rem",
      display: "flex",
      position: "relative",
      top: 0,
      width: "5rem",
      height: "8rem",
      border: `0.2rem solid ${palette.white}`,
      borderRadius: "0.5rem",
      alignItems: "center",
      alignContent: "center",
      justifyContent: "center",
      boxShadow: `0 0.2rem 0.3rem ${palette.veryDarkGray}`,
      margin: "0 0.6rem 0 0",
      cursor: "pointer",
      transitionProperty: "top",
      transitionDuration: "0.2s",
      backfaceVisibility: "hidden",

      "&:hover": {
        background: palette.darkGreen,
        color: palette.white,
      },

      "&.selected": {
        background: palette.brown,
        color: palette.white,
        top: "-1.5rem",
      },
    },
  }),
);