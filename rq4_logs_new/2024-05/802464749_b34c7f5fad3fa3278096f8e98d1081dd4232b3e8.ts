import { useTheme } from "@mui/material/styles";
import useMediaQuery from "@mui/material/useMediaQuery";

export function useChartWidth() {
  const theme = useTheme();
  const isVerySmallScreen = useMediaQuery(theme.breakpoints.down("xs"));
  const isSmallScreen = useMediaQuery(theme.breakpoints.between("xs", "sm"));
  const isMediumScreen = useMediaQuery(theme.breakpoints.between("sm", "md"));

  let chartWidth;
  if (isVerySmallScreen) {
    chartWidth = 100;
  } else if (isSmallScreen) {
    chartWidth = 300;
  } else if (isMediumScreen) {
    chartWidth = 400;
  } else {
    chartWidth = 700;
  }

  return chartWidth;
}