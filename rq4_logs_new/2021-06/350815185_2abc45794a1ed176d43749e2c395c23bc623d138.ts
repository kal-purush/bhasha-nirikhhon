import { EventEmitter } from 'events';
import { Breakpoints } from '~components/Breakpoints';
import { ComponentInterface } from '~common/scripts/modules/Component/types';

const el = document.querySelector('#app');
const emitter = new EventEmitter();
const debug = console.log;
let current = null;
const breakpoints = new Breakpoints({
  xxs: 0,
  xs: 475,
  sm: 768,
  md: 1024,
  lg: 1200,
  xl: 1366,
  xxl: 1440,
});
breakpoints.change((point: string) => debug(`breakpoint: ${point}`));

export const App = {
  debug,
  breakpoints,
  user: undefined,
  on: emitter.on,
  off: emitter.off,
  emit: emitter.emit,
  init(component: ComponentInterface): void {
    if (current) {
      current.unmount();
    }
    if (el) {
      el.textContent = '';
      component.mount(el);
      current = component;
    }
  },
};