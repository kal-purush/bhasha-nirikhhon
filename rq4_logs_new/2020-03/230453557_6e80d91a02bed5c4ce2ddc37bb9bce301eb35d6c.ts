import { createMuiTheme } from "@material-ui/core/styles";

export const MaterialUITheme = createMuiTheme({
  shape: {
    borderRadius: 0
  },
  palette: {
    primary: {
      main: "#000000"
    }
  },
  typography: {
    fontSize: 22,
    fontFamily: "inherit"
  },
  overrides: {
    MuiInput: {
      underline: {
        "&:before, &:after": {
          "border-width": "1px !important"
        },
        "& div": {
          top: "calc(50% - 18px)"
        }
      }
    }
  }
});