/** @jsxImportSource @emotion/react */
import { styled, useTheme  } from "@mui/material/styles";


export const CustomCheckbox = styled('input')(() => {
  const theme = useTheme(); 
  return {
    "-webkit-appearance": "none",
    "-moz-appearance": "none",
    appearance: "none",
    background: "#DADADA",
    cursor: "pointer",
    height: "16px",
    outline: "0",
    width: "16px",

    "&:after": {
      border: `solid ${theme.palette.common.white}`,
      borderWidth: "0 1px 1px 0",
      content: '""',
      display: "block",
      height: "45%",
      left: "35%",
      position: "relative",
      top: "20%",
      transform: "rotate(45deg)",
      width: "25%",
    },

    "&:checked": {
      background: "#333",
    },

    "&:checked:after": {
      display: "block",
    },
  };
});