module TECT {

    export class CountdownTimer {
        private _duration: number;
        private _granularity: number;
        private _remaining: Date;
        private _isRunning: boolean;
        private _element: HTMLElement;
        private _startTime: Date;
        private _tickEvents: Array<IManagedTickEvent>;
        private _onReset: { (): void; }

        constructor(duration: number, granularity: number = 1000, element?: HTMLElement) {
            this._duration = duration;
            this._granularity = granularity;
            this._remaining = new Date(duration);

            if (element)
                this._element = element;
        }

        set onReset(callback: { (): void; }) {
            this._onReset = callback;
        }

        set tickEvents(tickEvents: Array<ITickEvent>) {
            if (tickEvents && tickEvents.length > 0) {
                this._tickEvents = [];
                tickEvents.forEach((tickEvent) => {
                    this._tickEvents.push({
                        time: tickEvent.time,
                        callback: tickEvent.callback,
                        done: false
                    });
                });
            }
        }

        get remaining(): Date {
            return this._remaining;
        }

        get AsString(): string {
            var min: number = this._remaining.getMinutes();
            var sec: number = this._remaining.getSeconds();
            return (min < 10 ? '0' + min.toString() : min.toString()) + ':' +
                (sec < 10 ? '0' + sec.toString() : sec.toString());
        }

        private run() {
            if (this._isRunning) {
                this._remaining = new Date(this._duration - (new Date().getTime() - this._startTime.getTime()));

                if (this._remaining.getTime() > 0) {
                    setTimeout(() => this.run(), this._granularity);
                }
                else {
                    this._remaining = new Date(0);
                    this._isRunning = false;
                }

                this.updateUI();

                if (this._tickEvents && this._tickEvents.length > 0) {
                    this._tickEvents.filter((e) => { return !e.done && e.time > this._remaining.getTime() - this._granularity; }).forEach((e) => {
                        e.callback(e.time, this._element);
                        e.done = true;
                    });
                }

            }
        }

        private updateUI() {
            if (this._element) {
                this._element.innerHTML = this.AsString;
            }
        }

        public start() {
            this._isRunning = true;
            this._startTime = new Date();
            this.run();
        }

        public stop() {
            this._isRunning = false;
        }

        public reset() {
            this._tickEvents.forEach((e) => { e.done = false; });
            this._startTime = new Date();
            this._remaining = new Date(this._duration);
            this.updateUI();
            if (this._onReset)
                this._onReset();
        }

    }

    export interface ITickEvent {
        time: number;
        callback: { (time: number, element: HTMLElement): void; }
    }

    interface IManagedTickEvent extends ITickEvent {
        done: boolean;
    }

}