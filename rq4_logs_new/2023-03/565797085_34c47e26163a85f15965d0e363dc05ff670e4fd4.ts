import { Theme } from "@mui/material";

export const getComponentOverrides = (theme: Theme) => ({
  MuiFormLabel: {
    styleOverrides: {
      root: {
        color: theme.palette.mode === "light" ? "#25252D" : "#EBEBEF",
      },
    },
  },
  MuiInputBase: {
    styleOverrides: {
      root: {
        borderColor: theme.palette.mode === "dark" ? "#25252d" : "#d8d8df",
        borderRadius: "8px",
        backgroundColor: theme.palette.background.paper,
      },

    },
  },
  MuiBackdrop: {
    styleOverrides: {
      root: {
        backdropFilter: "blur(8px)",
        backgroundColor: "rgb(37 37 45 / 50%)",
      },
    },
  },
  MuiButton: {
    styleOverrides: {
      root: {
        backgroundColor: theme.palette.primary.main,
        fontWeight: 600,
        fontSize: "0.875rem",
        fontFamily: theme.typography.fontFamily,
        borderRadius: "0.5rem",
        textTransform: "none" as const,
      },
      outlined: {
        backgroundColor: "transparent",
        color: theme.palette.text.primary,
        borderColor: theme.palette.mode === "light" ? "rgb(0 0 0 / 23%)" : "#25252d",
        "&:hover": {
          borderColor: theme.palette.mode === "light" ? "#b9b9c6" : "#434356",
          background: theme.palette.mode === "light" ? "#ebebef" : "#25252d",
        },
      },
    },
  },
  MuiOutlinedInput: {
    styleOverrides: {
      // Name of the slot
      input: {
        padding: "0.5rem 0.75rem",
        minHeight: "2.5rem",

      },
    },
  },
});