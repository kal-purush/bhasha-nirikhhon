/**
 * A class that represents a Clock
 */
class Clock {
    /**
     * The value for hours on this clock. 0-11.
     */
    public hours: number;
    
    /**
     * The value for minutes on this clock. 0-59.
     */
    public minutes: number;
    
    /**
     * The value for seconds on this clock. 0-59.
     */
    public seconds: number;
    
    /**
     * Creates a new clock set to midnight (00:00:00). By default, it is set to tick foward.
     * To create a backwards clock, send false as the argument to the constructor.
     */
    constructor(public ticksForward: boolean = true) {
        this.hours = 0;
        this.minutes = 0;
        this.seconds = 0;
    }
    
    /**
     * Advances the time values by one tick.
     * If the clock is a forwards clock, everything is appropriately incremented in the forward direction.
     * If the clock is a backwards clock, everything is appropriately incremented in the backwards direction. (decremented)
     */
    tick(): void {
        // If it ticks forward, increment by 1. If it doesn't, increment by -1 (AKA decrement by 1).
        this.seconds += (this.ticksForward) ? 1 : -1;

        if (this.ticksForward) {
            if (this.seconds === 60) {
                this.seconds = 0;
                this.minutes++;
            }
            if (this.minutes === 60) {
                this.minutes = 0;
                this.hours++;
            }
            if (this.hours === 12) {
                this.hours = 0;
            }
        } else {
            if (this.seconds === -1) {
                this.seconds = 59;
                this.minutes--;
            }
            if (this.minutes === -1) {
                this.minutes = 59;
                this.hours--;
            }
            if (this.hours === -1) {
                this.hours = 11;
            }
        }
    }
    
    /**
     * Returns true/false whether this clock is equal to another clock.
     * Two clocks are equal if their seconds, minutes, and hours values are all equal.
     */
    equals(anotherClock: Clock): boolean {
        return this.seconds === anotherClock.seconds && this.minutes === anotherClock.minutes && this.hours === anotherClock.hours;
    }
}

// Create a good clock that ticks forward, a bad clock that ticks backwards.
// Create a variable to store the number of seconds in a day (ticks).
// Create a variable to store the number of matches that have occured.
let goodClock: Clock = new Clock(),
badClock: Clock = new Clock(false),
secondsInDay = 60 * 60 * 24,
matchCount = 0;

// This happens really fast. We just care that each clock is ticked once for each second in a day.
for (let i = 0; i < secondsInDay; ++i) {
    // If the clocks are equal, increment the match count. Do this first, because the clocks start out equal.
    if (goodClock.equals(badClock)) {
        ++matchCount;
    }
    goodClock.tick();
    badClock.tick();
}

console.log(`The clocks matched up ${matchCount} times.`);