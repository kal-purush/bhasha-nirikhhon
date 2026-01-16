import makeTypegraphyAccent from "./modules/make-typegraphy-accent.js";

const defaultAnimationRules = {
  name: `fadeAccentUp`,
  duration: 1,
  delay: 0,
  timingFunction: `ease-out`,
};

export default () => {
  const introTitle = document.querySelector(`.intro__title`);
  const introDate = document.querySelector(`.intro__date`);
  const sliderTitle = document.querySelector(`.slider__item-title`);
  const prizesTitle = document.querySelector(`.prizes__title`);
  const rulesTitle = document.querySelector(`.rules__title`);
  const gameTitle = document.querySelector(`.game__title`);

  makeTypegraphyAccent(introTitle, {
    ...defaultAnimationRules,
    lines: {
      1: {
        1: 0.2,
        2: 0.1,
        3: 0,
        4: 0.1,
        5: 0.2,
        6: 0.1,
        7: 0,
        8: 0.3,
        9: 0.1,
        10: 0,
        11: 0.1,
        12: 0.05,
      },
      2: {
        1: 0.4,
        2: 0.5,
        3: 0.4,
        4: 0.3,
        5: 0.4,
        6: 0.35,
      },
    },
  });

  makeTypegraphyAccent(introDate, {
    ...defaultAnimationRules,
    duration: 0.5,
    delay: 1.25,
    lines: {
      1: {
        1: 0.15,
        2: 0.1,
      },
      2: {
        1: 0.1,
      },
      3: {
        1: 0.15,
        2: 0.05,
        3: 0.15,
        4: 0.1,
        5: 0.2,
      },
      4: {
        1: 0.1,
      },
      5: {
        1: 0,
        2: 0.15,
        3: 0.1,
        4: 0.05,
      },
    },
  });

  makeTypegraphyAccent(sliderTitle, {
    ...defaultAnimationRules,
    duration: 0.75,
    lines: {
      1: {
        1: 0.25,
        2: 0.1,
        3: 0,
        4: 0.1,
        5: 0.2,
        6: 0.1,
        7: 0,
      },
    },
  });

  makeTypegraphyAccent(prizesTitle, {
    ...defaultAnimationRules,
    duration: 0.5,
    delay: 0.5,
    lines: {
      1: {
        1: 0.25,
        2: 0.1,
        3: 0,
        4: 0.1,
        5: 0.2,
      },
    },
  });

  makeTypegraphyAccent(rulesTitle, {
    ...defaultAnimationRules,
    duration: 0.5,
    lines: {
      1: {
        1: 0.25,
        2: 0.15,
        3: 0.1,
        4: 0,
        5: 0.2,
        6: 0.15,
        7: 0,
      },
    },
  });

  makeTypegraphyAccent(gameTitle, {
    ...defaultAnimationRules,
    duration: 0.75,
    lines: {
      1: {
        1: 0.25,
        2: 0.15,
        3: 0,
        4: 0.1,
      },
    },
  });
};