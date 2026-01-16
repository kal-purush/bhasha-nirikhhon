var giocatore = 1;
var g1 = "G1";
var g2 = "G2";
var numeroMosse = 0;
var finish = false;
var victory = new Audio("Victory.mp3");
var fail = new Audio("Fail.mp3");

function clickCasella(id) {
    var button = document.getElementById(id);
    if (button.type !== "image") {
        var img = "";
        var name = "";

        if (giocatore === 1) {
            img = "x.png";
            name = g1;
            giocatore = 2;
        } else {
            img = "o.png";
            name = g2;
            giocatore = 1;
        }

        numeroMosse++;
        button.type = "image";
        button.src = img;
        button.name = name;
        if (numeroMosse >= 5) {
            controllaVincitore();
        }
    }
}

function btnRipristinaClick() {
    location.reload();
}

function controllaVincitore() {
    var c1 = document.getElementById("casella1");
    var c2 = document.getElementById("casella2");
    var c3 = document.getElementById("casella3");
    var c4 = document.getElementById("casella4");
    var c5 = document.getElementById("casella5");
    var c6 = document.getElementById("casella6");
    var c7 = document.getElementById("casella7");
    var c8 = document.getElementById("casella8");
    var c9 = document.getElementById("casella9");
    
    var esito = document.getElementById("Esito");

    if (c1.name === g1 && c2.name === g1 && c3.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c4.name === g1 && c5.name === g1 && c6.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c7.name === g1 && c8.name === g1 && c9.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c1.name === g1 && c4.name === g1 && c7.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c2.name === g1 && c5.name === g1 && c8.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c3.name === g1 && c6.name === g1 && c9.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c1.name === g1 && c5.name === g1 && c9.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c3.name === g1 && c5.name === g1 && c7.name === g1) {
        finish = true;
        esito.innerHTML = "Le X hanno vinto!";
    } else if (c1.name === g2 && c2.name === g2 && c3.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c4.name === g2 && c5.name === g2 && c6.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c7.name === g2 && c8.name === g2 && c9.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c1.name === g2 && c4.name === g2 && c7.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c2.name === g2 && c5.name === g2 && c8.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c3.name === g2 && c6.name === g2 && c9.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c1.name === g2 && c5.name === g2 && c9.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    } else if (c3.name === g2 && c5.name === g2 && c7.name === g2) {
        finish = true;
        esito.innerHTML = "Le O hanno vinto!";
    }

    if (finish) {
        c1.setAttribute("disabled", "true");
        c2.setAttribute("disabled", "true");
        c3.setAttribute("disabled", "true");
        c4.setAttribute("disabled", "true");
        c5.setAttribute("disabled", "true");
        c6.setAttribute("disabled", "true");
        c7.setAttribute("disabled", "true");
        c8.setAttribute("disabled", "true");
        c9.setAttribute("disabled", "true");
        victory.play();
    } else if (!finish && numeroMosse == 9) {
        esito.innerHTML = "Patta!";
        fail.play();
    }

}

function disableButtons(c1, c2, c3, c4, c5, c6, c7, c8, c9) {
    c1.setAttribute("disabled", "true");
    c2.setAttribute("disabled", "true");
    c3.setAttribute("disabled", "true");
    c4.setAttribute("disabled", "true");
    c5.setAttribute("disabled", "true");
    c6.setAttribute("disabled", "true");
    c7.setAttribute("disabled", "true");
    c8.setAttribute("disabled", "true");
    c9.setAttribute("disabled", "true");
}