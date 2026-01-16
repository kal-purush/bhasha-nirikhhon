import { makeStyles, createStyles, Theme } from "@material-ui/core";

export const headerStyles = makeStyles((theme: Theme) =>
  createStyles({
    appBar: {
      [theme.breakpoints.down("md")]: {
        height: "15%"
      },
      height: "66px"
    },
    menuButton: {
      display: "none"
    },
    title: {
      flexGrow: 1
    },
    text: {
      margin: "0.3rem",
      fontSize: "14px",
      lineHeight: "20px"
    },
    button: {
      borderRadius: "0",
      margin: "1rem",
      width: "5rem",
      backgroundColor: theme.palette.secondary.main,
      textTransform: "inherit",
      [theme.breakpoints.down("md")]: {
        margin: "0.2rem"
      }
    },
    icon: {
      margin: theme.spacing(1),
      height: "5rem",
      width: "5rem",
      fontSize: "large",
      [theme.breakpoints.down("md")]: {
        height: "4rem",
        weight: "4rem"
      }
    },
    toolBar: {
      display: "flex",
      width: "60%",
      height: "80%",
      flexDirection: "row",
      justifyContent: "space-between",
      alignSelf: "center",
      [theme.breakpoints.down("md")]: {
        width: "100%"
      }
    },
    ToolbarItem: {
      display: "flex",
      justifyContent: "space-evenly",
      alignItems: "center",
      [theme.breakpoints.down("md")]: {
        alignSelf: "center",
        marginRight: "10px"
      }
    }
  })
);