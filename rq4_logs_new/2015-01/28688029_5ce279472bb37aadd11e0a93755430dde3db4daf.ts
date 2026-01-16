/**
 * Copyright (C) 2015 yanni4night.com
 * speedometer.ts
 *
 * changelog
 * 2015-01-10[17:35:15]:revised
 * 
 * @author yanni4night@gmail.com
 * @version 0.1.0
 * @since 0.1.0
 */

interface SpeedoMeterListener{
    onReport(position: number, speed: number): void;
}

interface SpeedoMeterReporter{
}

/**
 * @class SpeedMeter
 * @version 0.1.0
 * @since 0.1.0
 */
class SpeedoMeter implements SpeedoMeterReporter{
    private mass: number;//Mass
    private speed: number = 0;//Speed
    private position: number;
    private acceleration: number;//Acceleration

    private isRunning: boolean = false;
    private timer: any;

    private clockPeriod: number;//millisecond

    private listeners: Array<SpeedoMeterListener> = [];

    constructor(mass: number = 2000, acceleration: number = 2, position: number = 0, clockPeriod: number = 50){
        this.mass = mass;
        this.acceleration = Math.abs(acceleration);
        this.position = position;

        this.clockPeriod = clockPeriod;
    }

    private tick(){
        if(Math.abs(this.speed) < this.acceleration){
            this.speed = 0;
        }else if(this.speed > 0){
            this.speed += -this.acceleration;
        }else if(0 > this.speed){
            this.speed += this.acceleration;
        }
        
        if(Math.abs(this.speed) < 1e-3){
            this.speed = 0;
        }

        this.report(this.position += this.speed, this.speed);
    }

    private report(position: number, speed: number){
        this.listeners.forEach( listener => {
            listener.onReport(position, speed);
            });
    }

    addListener(listener: SpeedoMeterListener){
        if(!~this.listeners.indexOf(listener)){
            this.listeners.push(listener);
        }
        return listener;
    }
    /**
     * Alias for <b>run</b>.
     * 
     * @version 0.1.0
     */
    start(){
        return this.run.apply(this, arguments);
    }
    /**
     * Start the "speedometer".
     * 
     * @param {delay}
     * @returns this
     * @version 0.1.0
     */
    run(delay: number = 0){
        var start;

        if(this.isRunning){
            return this;
        }
        //Set to true immediately due to the possible 'delay'
        this.isRunning = true;

        start = function(){
                this.timer = setInterval(this.tick.bind(this), this.clockPeriod);
        }.bind(this);

        if(delay){
            setTimeout(start, delay);
        }else{
            start();
        }

        return this;
    }
    /**
     * A momentum on the mass.
     * 
     * @param {momentum}
     * @returns this
     * @version 0.1.0
     */
    push(momentum: number = 0){
        if(!this.isRunning || !momentum){
            return this;
        }

        this.speed += momentum / this.mass;

        return this;
    }
}