import { DOMTemplate } from "./base-component";

class TimeClock extends DOMTemplate {
  //single singleton
  timer: any;
  time: Date | null = null;
  timeDOM: HTMLElement | null = null;
  timeFormatDOM: HTMLElement | null = null;

  private static instance: TimeClock;

  private constructor() {
    super("timer");
  }
  static getInstance() {
    if (!TimeClock.instance) {
      TimeClock.instance = new TimeClock();
    }
    return TimeClock.instance;
  }

  startTimeChange(id: string, id2: string) {
    this.time = new Date();
    const localTime = this.time.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
    this.timeDOM = document.getElementById(id)!;
    this.timeFormatDOM = document.getElementById(id2)!;
    //init
    this.timeDOM.innerHTML = localTime;

    this.timer = setInterval(() => {
      const tempTime = new Date();
      const localTime = tempTime.toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });

      if (
        localTime !==
        this.time!.toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        })
      ) {
        console.log("lets changed");
        this.timeDOM!.innerHTML = localTime;
      }
      this.time = tempTime;
    }, 1000);
  }
  setTimeFormat() {}
}
const timeClock = TimeClock.getInstance();
export default timeClock;