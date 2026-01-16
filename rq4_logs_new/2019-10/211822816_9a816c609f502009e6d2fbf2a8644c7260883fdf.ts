import Event from "rx.mini";

type Stream = { v: ArrayBuffer; t: number }[];

export class StreamPlayer {
  que: Stream[] = [];

  event = new Event<ArrayBuffer>();

  constructor() {}

  push(arr: Stream) {
    this.que.push(arr);
    if (this.que.length === 1) this.play();
  }

  async play() {
    const stream = this.que.shift();
    if (stream) {
      for (let { v, t } of stream) {
        console.log({ t });
        await new Promise(r => setTimeout(r, t));
        this.event.execute(v);
      }
      await this.play();
    }
  }
}