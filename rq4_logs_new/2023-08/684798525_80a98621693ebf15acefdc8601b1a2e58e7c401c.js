//Class for Pomodor's lapses
const lapses = {
    break: 0,
    longBreak: 0
}
//Prototype Clock
class Clock {
    constructor({
        hour,
        minutes,
        seconds
    }){
        this.hour = hour;
        this.minutes = minutes,
        this.seconds = seconds
    }       
}
//Instance Clock
const globalClock = new Clock ({
    hour: 0,
    minutes: 0,
    seconds: 0,
})

const pomodoroClock = new Clock ({
    hour: 0,
    minutes: 0,
    seconds: 0,
    }
)

//Functions for lapses
const pomodoroInterval = (clock) => {
    setInterval(() => {
    if(lapses.longBreak == 4){
        if(clock.seconds > 58){
            clock.seconds = 0
            if(clock.minutes > 13){
                clock.minutes = 0
                clock.seconds = 0
                lapses.break = 0
                lapses.longBreak = 0
            }else{clock.minutes++}
        }else{clock.seconds++}
    }else{
        //Count until 5 min
        if(lapses.break === 1){
            if(clock.seconds > 58){
                clock.seconds = 0
                if(clock.minutes > 3){
                    clock.minutes = 0
                    clock.seconds = 0
                    lapses.break--
                }else{clock.minutes++}
            }else{clock.seconds++}
        }
        //Count until 25 min
        else{if(clock.seconds > 58){
            clock.seconds = 0
            if(clock.minutes > 23){
                clock.minutes = 0
                clock.seconds = 0
                lapses.break++
                lapses.longBreak++
            } else{clock.minutes++}
        } else{clock.seconds++}}
    }
        console.log(`Pomodoro${clock.hour}:${clock.minutes}:${clock.seconds}`)}, 10)
}
const globalInterval = (clock) => {setInterval(() => {
    if(clock.seconds > 58){
        clock.seconds = 0
        if(clock.minutes > 58){
            clock.minutes = 0
            if(clock.hour > 22) {
                clock.hour = 0
            }else{clock.hour++}
        }
        else{clock.minutes++}
    }else{clock.seconds++}
    
    console.log(`${clock.hour}:${clock.minutes}:${clock.seconds}`)}, 1000)
}


globalInterval(globalClock)
pomodoroInterval(pomodoroClock)