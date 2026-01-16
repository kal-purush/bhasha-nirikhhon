/**
 * MyAppIn (http://www.myappin.cz)
 *
 * @author    Tomas Hyl (tomas@hyl-design.com)
 * @copyright Copyright (c) 2017 MyAppIn, s. r. o. (http://www.myappin.cz)
 */

import { animate, animation, style } from "@angular/animations";

const defaultParams = {
  enterDuration: "233ms",
  enterTiming: "ease-out",
  leaveDuration: "233ms",
  leaveTiming: "ease-in",
  movementXaxis: "100%",
  movementYaxis: "0",
  movementZaxis: "233px",
  perspective: "144px",
};

export const slideInFromLeft = animation([
  style({
    transform: "perspective({{perspective}}) translate3d(-{{movementXaxis}}, -{{movementYaxis}}, -{{movementZaxis}})",
    opacity: 0
  }),
  animate("{{enterDuration}} {{enterTiming}}",
    style({
      transform: "translate(0, 0)",
      opacity: 1
    })
  ),
], {
  params: defaultParams,
});

export const slideOutToLeft = animation([
  style({
    transform: "perspective({{perspective}}) translate3d(0, 0, -{{movementZaxis}})",
    opacity: 1
  }),
  animate("{{leaveDuration}} {{leaveTiming}}",
    style({
      transform: "translate(-{{movementXaxis}}, -{{movementYaxis}})",
      opacity: 0
    })
  ),
], {
  params: defaultParams,
});

export const slideInFromRight = animation([
  style({
    transform: "perspective({{perspective}}) translate3d({{movementXaxis}}, -{{movementYaxis}}, -{{movementZaxis}})",
    opacity: 0
  }),
  animate("{{enterDuration}} {{enterTiming}}",
    style({
      transform: "translate(0, 0)",
      opacity: 1
    })
  ),
], {
  params: defaultParams,
});

export const slideOutToRight = animation([
  style({
    transform: "perspective({{perspective}}) translate3d(0, 0, -{{movementZaxis}})",
    opacity: 1
  }),
  animate("{{leaveDuration}} {{leaveTiming}}",
    style({
      transform: "translate({{movementXaxis}}, -{{movementYaxis}})",
      opacity: 0
    })
  ),
], {
  params: defaultParams,
});