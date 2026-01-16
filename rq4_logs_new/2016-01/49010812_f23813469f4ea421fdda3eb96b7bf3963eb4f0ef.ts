
interface IAction {
    (): void;
};
type milisecond = number;


export class Interval {
    private action:IAction;
    private time: milisecond;
    private active: boolean = false;
    private interval = null;
    private autoOnInterval = null;

    constructor(action, time) {
        this.action = action;
        this.time = time;
    }

    getTime(): milisecond {
        return this.time;
    }

    setTime(newTime: milisecond) {
        if (!this.active) {
            this.time = newTime;
        } else {
            this.stop();
            this.time = newTime;
            this.run();
        }
    }

    private switchState() {
        this.active = !this.active;
    }

    stop() {
        if (!this.interval) {
            return;
        }

        clearInterval(this.interval);
        this.interval = null;
        this.switchState();
    }

    run() {
        if (!this.active) {
            var self = this;
            var interval = setInterval(function() {
                self.action()
            }, this.time);

            this.interval = interval;
            this.switchState();
        }
    }

    autoOn(time: milisecond) {
        this.stop();
        var autoOnInterval = setInterval(() => {
            this.run();
            this.autoOnInterval = null;
        }, time);

        this.autoOnInterval = autoOnInterval;

    }

}