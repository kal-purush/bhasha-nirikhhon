interface IdleView {
  id: "default";
  subView: null;
}

export interface RingView {
  id: "ring";
  subView: "sound" | "silent";
}

interface TimerView {
  id: "timer";
  subView: null;
}

export type View = IdleView | RingView | TimerView;