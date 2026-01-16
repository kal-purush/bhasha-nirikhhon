import { makeStyles } from "@mui/styles";

export const useStyles = makeStyles({
  brand: {
    display: "flex",
    alignItems: "center",
    gap: "10px",
    padding: "30px 15px",
    cursor: "pointer",
  },
  navItem: {
    "&:hover": {
      backgroundColor: "#1900D5 !important",
      color: "#fff",
      borderRadius: "4px",
    },
  },
});