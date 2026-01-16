document.addEventListener("DOMContentLoaded", function() {
    document.querySelector(".clock .hours").innerHTML = 
        new Array(24).fill(0).map((n,i)=>{
            return `
                <div>
                    ${i<10 ? "0"+i : i}
                </div>
            `;
        }).join("");

    document.querySelector(".clock .minutes").innerHTML = 
        new Array(60).fill(0).map((n,i)=>{
            return `
                <div>
                    ${i<10 ? "0"+i : i}
                </div>
            `;
        }).join("");

    document.querySelector(".clock .seconds").innerHTML = 
        new Array(60).fill(0).map((n,i)=>{
            return `
                <div>
                    ${i<10 ? "0"+i : i}
                </div>
            `;
        }).join("");

    let time = new Date();
    let prevHours = time.getHours() * 100;
    let prevMinutes = time.getMinutes() * 100;
    let prevSeconds = time.getSeconds() * 100;

    document.querySelector(".clock .hours div").style.marginTop = `-${prevHours}px`;
    document.querySelector(".clock .minutes div").style.marginTop = `-${prevMinutes}px`;
    document.querySelector(".clock .seconds div").style.marginTop = `-${prevSeconds}px`;

    function updateClock() {
        let time = new Date();
        let hours = time.getHours() * 100;
        let minutes = time.getMinutes() * 100;
        let seconds = time.getSeconds() * 100;

        if(prevHours !== hours){
            prevHours = hours;
            document.querySelector(".clock .hours div").style.marginTop = `-${prevHours}px`;
            document.querySelector(".clock .hours").classList.add("animate");
            setTimeout(function(){
                document.querySelector(".clock .hours").classList.remove("animate");
            },800);
        }

        if(prevMinutes !== minutes){
            prevMinutes = minutes;
            document.querySelector(".clock .minutes div").style.marginTop = `-${prevMinutes}px`;
            document.querySelector(".clock .minutes").classList.add("animate");
            setTimeout(function(){
                document.querySelector(".clock .minutes").classList.remove("animate");
            },800);
        }

        if(prevSeconds !== seconds){
            prevSeconds = seconds;
            document.querySelector(".clock .seconds div").style.marginTop = `-${prevSeconds}px`;
            document.querySelector(".clock .seconds").classList.add("animate");
            setTimeout(function(){
                document.querySelector(".clock .seconds").classList.remove("animate");
            },800);
        }
    }

    setInterval(updateClock, 1000);
});