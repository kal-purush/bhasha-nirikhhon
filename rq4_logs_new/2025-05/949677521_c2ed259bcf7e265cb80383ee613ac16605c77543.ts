import { red } from "@mui/material/colors";

export const styles = {
  root: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    padding: 2,
    gap: 2,
    textAlign: "center",
    marginTop: 3,
  },
  favoriteIcon: {
    bgcolor: red[500],
    width: 48,
    height: 48,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  title: {
    display: "flex",
    alignItems: "center",
    gap: 1,
    fontWeight: "bold",
  },
  tagline: {
    fontStyle: "italic",
    color: "gray",
  },
};