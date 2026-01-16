export enum Breakpoint {
  xxxs = 320,
  xxs = 375,
  xs = 425,
  s = 640,
  m = 768,
  l = 1024,
  xl = 1280,
  xxl = 1440,
  xxxl = 1920,
}

const down = (bp: number) => `@media screen and (max-width: ${bp - 0.02}px)`;
const up = (bp: number) => `@media screen and (min-width: ${bp}px)`;

const Up = {
  xxxs: up(Breakpoint.xxxs),
  xxs: up(Breakpoint.xxs),
  xs: up(Breakpoint.xs),
  s: up(Breakpoint.s),
  m: up(Breakpoint.m),
  l: up(Breakpoint.l),
  xl: up(Breakpoint.xl),
  xxl: up(Breakpoint.xxl),
  xxxl: up(Breakpoint.xxxl),
};

const Down = {
  xxxs: down(Breakpoint.xxxs),
  xxs: down(Breakpoint.xxs),
  xs: down(Breakpoint.xs),
  s: down(Breakpoint.s),
  m: down(Breakpoint.m),
  l: down(Breakpoint.l),
  xl: down(Breakpoint.xl),
  xxl: down(Breakpoint.xxl),
  xxxl: down(Breakpoint.xxxl),
};

export const Media = {
  up: Up,
  down: Down,
};