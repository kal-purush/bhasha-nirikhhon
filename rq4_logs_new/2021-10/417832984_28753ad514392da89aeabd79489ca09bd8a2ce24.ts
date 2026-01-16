import { createTheme } from "@mui/material/styles";

const theme = createTheme({
  palette: {
    primary: {
      light: "rgb(120, 178, 245)",
      main: "rgb(41, 114, 196)",
      dark: "rgb(35, 68, 140)",
      contrastText: "rgb(230, 238, 247)",
    },
    secondary: {
      light: "rgb(228, 152, 235)",
      main: "rgb(157, 55, 166)",
      dark: "rgb92, 35, 97)",
      contrastText: "rgb(243, 223, 245)",
    },
    // success: {},
    // warning: {},
    // error: {},
  },
});

export default theme;