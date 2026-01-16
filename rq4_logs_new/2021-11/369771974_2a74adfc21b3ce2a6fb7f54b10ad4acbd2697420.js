function startTimer(duration) {
    let display = document.getElementById("timer");
    if (display.textContent != "00:00") {
        alert("timer reset");
        let timer = duration, minutes, seconds;
        clearInverval(refresh);

    }
    let timer = duration, minutes, seconds;
    let refresh = setInterval(function () {
        minutes = parseInt(timer / 60, 10)
        seconds = parseInt(timer % 60, 10);

        minutes = minutes < 10 ? "0" + minutes : minutes;
        seconds = seconds < 10 ? "0" + seconds : seconds;

        display.textContent = minutes + ":" + seconds;

        if (--timer < 0) {
            display.textContent = "00" + ":" + "00";
            
        }
    }, 1000);
}

function startMyTimer(durata) {
    let minuti = durata / 60;
    let secondi = durata % 60;
    let parag = document.getElementById("timer");
    setInterval(() => {
        if(secondi =="00") {
            if (--minuti == "00") {
                break;
            }
            minuti--;
        }
    }, 1000);
}