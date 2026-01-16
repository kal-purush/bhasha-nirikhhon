type Callback = () => void;

let listenersPlay: Callback[] = [];
let listenersPause: Callback[] = [];

export const control = {
  play() {
    listenersPlay.forEach(it => it());
  },
  pause() {
    listenersPause.forEach(it => it());
  }
};

export function addEventListenerPlay(callback: Callback) {
  listenersPlay.push(callback);
}

export function addEventListenerPause(callback: Callback) {
  listenersPause.push(callback);
}

export function removeEventListenerPlay(callback: Callback) {
  listenersPlay = listenersPlay.filter(it => it !== callback);
}

export function removeEventListenerPause(callback: Callback) {
  listenersPause.push(callback);
}