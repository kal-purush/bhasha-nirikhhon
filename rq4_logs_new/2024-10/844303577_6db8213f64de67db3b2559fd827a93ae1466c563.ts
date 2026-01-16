import { useContext } from "react";

import { BreakpointsContext } from "@/providers/BreakpointsProvider";

const useBreakpoints = () => {
  const breakpoints = useContext(BreakpointsContext);

  if (!breakpoints) {
    throw new Error("useBreakpoints must be used within a BreakpointsProvider");
  }

  return breakpoints;
};

export default useBreakpoints;