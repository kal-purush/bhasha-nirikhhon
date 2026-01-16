import { WinSize } from 'ctx/WinSize';
import { CountdownLevel } from 'ctx/Countdown';
import { backgroundApplyParams } from 'cnt/ProgressiveImg';

import xs from './assets/bg_xs.jpg';
import sm from './assets/bg_sm.jpg';
import md from './assets/bg_md.jpg';
import lg from './assets/bg_lg.jpg';
import xl from './assets/bg_xl.jpg';

export const params = {
  [WinSize.XS]: {
    [CountdownLevel.CORE]: xs,
  },
  [WinSize.SM]: {
    [CountdownLevel.CORE]: sm,
  },
  [WinSize.MD]: {
    [CountdownLevel.CORE]: md,
  },
  [WinSize.LG]: {
    [CountdownLevel.CORE]: lg,
  },
  [WinSize.XL]: {
    [CountdownLevel.CORE]: xl,
  },
};

export default backgroundApplyParams(params);