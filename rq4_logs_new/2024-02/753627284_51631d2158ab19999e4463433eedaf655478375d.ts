import { ButtonType, ButtonStyle } from "./types";
import { Colors } from "../../enums/Colors";

const primaryButton: ButtonStyle = {
  bgcolor: Colors.royalBlue,
  borderRadius: "20px",
  color: Colors.white,
  pl: "20px",
  pr: "20px",
  fontSize: "14px",
};

const secondaryButton: ButtonStyle = {
  bgcolor: Colors.lavenderGray,
  borderRadius: "20px",
  color: Colors.slateBlue,
  pl: "20px",
  pr: "20px",
  fontSize: "14px",
};

const textButton: ButtonStyle = {
  bgcolor: Colors.white,
  borderRadius: "20px",
  color: Colors.slateBlue,
  pl: "20px",
  pr: "20px",
  fontSize: "14px",
};

export const buttonType: ButtonType = {
  primary: primaryButton,
  secondary: secondaryButton,
  text: textButton,
};