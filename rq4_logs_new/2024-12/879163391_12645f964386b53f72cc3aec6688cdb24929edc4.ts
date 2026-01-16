import { useState } from "react";

type View = "ring" | "timer" | "idle";
let transitionInterval: ReturnType<typeof setTimeout> | null = null;

interface SetDynamicIslandViewOutput {
  dynamicIslandView: View;
  setViewWithTransition(nextView: View): void;
}

export function useSetDynamicIslandView(): SetDynamicIslandViewOutput {
  const [dynamicIslandView, setDynamicIslandView] = useState<View>("idle");

  function setViewWithTransition(nextView: View) {
    if (nextView === dynamicIslandView) {
      return;
    }

    if (transitionInterval) {
      clearInterval(transitionInterval);
    }

    if (nextView === "idle" || dynamicIslandView === "idle") {
      setDynamicIslandView(nextView);

      return;
    }

    setDynamicIslandView("idle");

    transitionInterval = setTimeout(() => setDynamicIslandView(nextView), 500);
  }

  return {
    dynamicIslandView,
    setViewWithTransition,
  };
}