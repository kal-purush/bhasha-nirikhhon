import { inject } from "./injection";

let observer: MutationObserver | null = null;

const checkElements = () => {
  const app = document.getElementById('app');
  return !!app && app.childElementCount > 0;
};

const startObservation = () => {
  observer = new MutationObserver(async (mutations) => {
    const changedVideo = "display: flex; height: 100%; width: 100%;";
    const isStyle = mutations[0].attributeName === "style";
    const isChangedVideo = (mutations[0].target as HTMLElement).getAttribute("style") === changedVideo;

    if (isStyle && isChangedVideo && !checkElements()) {
      console.log('video loaded');
      console.log('not injected yet');
      await inject();
    }
  });

  const vilosControlsContainer = document.getElementById(
    "player0",
  ) as HTMLElement;
  observer.observe(vilosControlsContainer, {
    attributes: true,
  });
};

startObservation();