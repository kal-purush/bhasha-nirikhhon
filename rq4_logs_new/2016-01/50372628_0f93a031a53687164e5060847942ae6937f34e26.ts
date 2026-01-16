/// <reference path="./scripts/callabike.ts" />

(function () {

        var header = document.getElementById("header");
        var splash = document.getElementById("splash");
        var info = document.getElementById("info");
        var button = <HTMLButtonElement> document.getElementById("startButton");
        button.addEventListener("click", () => {
            splash.hidden = true;
            info.hidden = false;
            CallABike.app.start();
        });
        
        CallABike.app.on(CallABike.Events.Clock, (counter: Date) => {
            header.innerHTML = counter.toLocaleString();
        });

        CallABike.app.on(CallABike.Events.LoadedChunk, () => {
            button.disabled = false;
        });

        var closeInterval = -1;
        CallABike.app.on(CallABike.Events.ClickedBike, (bike: CallABike.Bike) => {
            
            $('#info').css("right",0);
            $('#info > h1').text("Bike " + bike.id);
            $('#info > p').html(bike.moves.movements.map((m, i) => "Trip " + (i + 1) + " : " + m.duration).join("<br />"));
            clearInterval(closeInterval);
            closeInterval = setTimeout(() => $('#info').css("right",null), 3000);

        });

        CallABike.app.loadData();
})();