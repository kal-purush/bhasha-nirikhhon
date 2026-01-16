window.onload = () => {
    function buildClock() {
        const date = new Date();

        let hours = date.getHours(); // 0 - 23
        if (hours > 12) {
            hours -=12;
        }
        if (hours ===0) {
            hours = 12;
        }
        
        let minutes = date.getMinutes(); // 0 - 59
        let seconds = date.getSeconds(); // 0 - 59

        hours = hours < 10 ? "0" + hours : hours;
        minutes = minutes < 10 ? "0" + minutes : minutes;
        seconds = seconds < 10 ? "0" + seconds : seconds;

        document.querySelector("#clock-hours").innerHTML = hours;
        document.querySelector("#clock-minutes").innerHTML = minutes;
        document.querySelector("#clock-seconds").innerHTML = seconds;

        setTimeout(buildClock, 1000);
    }
    buildClock();
}