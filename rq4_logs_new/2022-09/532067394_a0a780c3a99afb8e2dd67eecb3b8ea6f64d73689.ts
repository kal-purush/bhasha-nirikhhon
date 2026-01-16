import { createVNode, render } from 'vue';
import TheNotification from './TheNotification.vue';

export function createNotification(msg: string): void {
  const node = createVNode(TheNotification, { msg });

  if (document.querySelector('.notification')) {
    const elPrev = document.querySelector('.notification');
    elPrev?.remove();
  }

  const el = document.createElement('div');
  el.className = 'notification';

  document.body.appendChild(el);

  render(node, el);

  setTimeout(() => {
    if (el) {
      el.remove();
    }
  }, 3000)
};