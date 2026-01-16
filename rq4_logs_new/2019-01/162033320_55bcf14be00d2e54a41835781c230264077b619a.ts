import blue from "@material-ui/core/colors/blue";
import grey from "@material-ui/core/colors/grey";
import red from "@material-ui/core/colors/red";

const primary = blue[300];
const dark = blue[700];
const gray: GrayScale = {
  G0: "#FFF",
  G1: grey[100],
  G2: grey[300],
  G3: grey[700],
  G4: grey[900],
  G5: "000"
};
const error = red[300];

interface GrayScale {
  G0: string;
  G1: string;
  G2: string;
  G3: string;
  G4: string;
  G5: string;
}

export interface Palette {
  primary: string;
  dark: string;
  error: string;
  gray: GrayScale;
}

const palette: Palette = {
  primary,
  dark,
  gray,
  error
};

export default palette;