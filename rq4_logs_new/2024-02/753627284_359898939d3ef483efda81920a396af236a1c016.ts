import { AppCards } from "./types";
import { Colors } from "../../globalStyle/Colors";
import { type SxProps } from "@mui/material";

const articleCard: SxProps = {
  maxWidth: "980px",
  height: "240px",
  border: "1px solid",
  borderColor: Colors.lavenderGray,
  borderRadius: "20px",
  boxShadow: "none",
};

const widgetCard: SxProps = {
  maxWidth: "412px",
  height: "380px",
  border: "1px solid",
  borderColor: Colors.lavenderGray,
  borderRadius: "20px",
  boxShadow: "none",
};

export const cardStyles: Record<AppCards, SxProps> = {
  article: articleCard,
  widget: widgetCard,
};