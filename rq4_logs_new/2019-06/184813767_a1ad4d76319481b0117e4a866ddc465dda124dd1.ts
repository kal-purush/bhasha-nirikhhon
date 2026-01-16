import { BodyScroll } from './types';

const stack: boolean[] = [];

export const bodyScroll: Readonly<BodyScroll> = {
  on: changeScroll.bind(this, true),

  off: changeScroll.bind(this, false),

  getStatus() {
    return !stack.length;
  }
};

function changeScroll(status: boolean) {
  if (status) {
    stack.pop();
  } else {
    stack.push(true);
  }
  window.requestAnimationFrame(changeBodyScroll);
}

function changeBodyScroll() {
  document.body.style.overflow = stack.length ? 'hidden' : null;
}