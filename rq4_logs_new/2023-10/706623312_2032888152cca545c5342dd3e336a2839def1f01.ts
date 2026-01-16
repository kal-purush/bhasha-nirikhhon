
const twentyFourHours = 86400000;

export function runAtSpecificTimeOfDay(hour: number,
                                        minuts: number, 
                                        fun: () => void, 
                                        updateInterval:(interval: NodeJS.Timeout) => void): NodeJS.Timeout {
    return setTimeout(() => {
            //first execution
            fun();
    
            //executions after
            updateInterval(setInterval(fun, twentyFourHours));
        }, calcRemainingTimeForFirstExecution(new Date(), hour, minuts));
    };

function calcRemainingTimeForFirstExecution(currentTime: Date, hour: number, minuts: number): number {
    let remainingTime = new Date(currentTime.getFullYear(),
                                currentTime.getMonth(),
                                currentTime.getDate(),
                                hour,
                                minuts,
                                0,0).getTime() - currentTime.getTime();

    if (remainingTime < 0)
        remainingTime += twentyFourHours;
    
    return remainingTime;
}