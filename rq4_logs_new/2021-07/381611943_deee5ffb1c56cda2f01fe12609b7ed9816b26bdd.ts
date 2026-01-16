import { RefObject, useState } from "react";
import useVisibilityChange from "../../../../../../hooks/useVisibilityChange";
import { sleep } from "../../../../../../utils";
import { LIST_ITEM_BACKGROUND_ANIMATION_DURATION } from "../../../../constants";

function useBackground(element: RefObject<HTMLDivElement>) {
  const [animationInProgress, setAnimationInProgress] = useState(false);
  const [windowInView] = useVisibilityChange();

  const handleCounterChange = async () => {
    if (!windowInView) return;
    if (animationInProgress) return;
    setAnimationInProgress(true);
    handleBg(true);
    await sleep(LIST_ITEM_BACKGROUND_ANIMATION_DURATION);
    handleBg(false);
    setAnimationInProgress(false);
  };

  const handleBg = (bool: boolean) => {
    if (!element || !element.current) return;
    if (bool) {
      element.current.classList.add("list-item-bg-active");
    } else {
      element.current.classList.remove("list-item-bg-active");
    }
  };

  return [handleCounterChange];
}

export default useBackground;