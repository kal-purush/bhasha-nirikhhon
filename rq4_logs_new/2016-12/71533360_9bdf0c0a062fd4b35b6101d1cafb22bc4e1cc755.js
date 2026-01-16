function validacijaUtiska() {
    var x = document.forms["utisci"]["kom"].value;
    if (x == null || x == "") {

        document.getElementById("greskaUtisak").innerHTML = "Niste unijeli poruku!";
        return false;
    } else {
        document.getElementById("greskaUtisak").innerHTML = "";
        return true;
    }

}

function prazanUnos(obj) {
    var y = document.forms["preplataforma"][obj.name].value;
    if (y == null || y == "") {

        document.getElementById("greskaPreplata").innerHTML = "Unesite " + obj.name + "!";
        document.getElementById("submitbutton").disabled = true;
    } else {
        document.getElementById("greskaUtisak").innerHTML = " ";
    }
}

function validacijaAnkete() {
    var radio = document.getElementsByName("anketa");
    var validna = false;
    var i = 0;
    while (!validna && i < radio.length) {
        if (radio[i].checked) validna = true;
        i++;
    }
    if (!validna) document.getElementById("greskaAnketa").innerHTML = "Odaberite opciju!";
    return validna;
}

function validacijaKontakta() {

    var ime = document.forms["preplataforma"]["ime"].value;
    var prezime = document.forms["preplataforma"]["prezime"].value;
    var telefon = document.forms["preplataforma"]["tel"].value;
    var mail = document.forms["preplataforma"]["mail"].value;
    var preplata = document.forms["preplataforma"]["preplatacheck"].checked;

    //minimalno 20, maximalno 30 karaktera- samo slova
    var regexImena = /^[a-zA-Z ]{2,30}$/;

    //format telefona mora biti xxx-xxx-xxx
    var regexTelefona = /^\(?(\d{3})\)?[-]?(\d{3})[-]?(\d{3})$/;

    //u formatu nesto@nesto.com
    var regexMaila = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;


    if (!regexImena.test(ime)) {
        document.getElementById("greskaPreplata").innerHTML = "Neispravan format imena, dozvoljeno je koristiti samo slova!";
        return false;

    }
    if (!regexImena.test(prezime)) {
        document.getElementById("greskaPreplata").innerHTML = "Neispravan format prezimena, dozvoljeno je koristiti samo slova!!";
        return false;

    }

    if (!regexTelefona.test(telefon)) {
        document.getElementById("greskaPreplata").innerHTML = "Format telefona mora biti xxx-xxx-xxx";
        return false;

    }
    if (!regexMaila.test(mail)) {
        document.getElementById("greskaPreplata").innerHTML = "Format maila mora biti nesto@nesto.com";
        return false;

    }
    if (!preplata) {
        document.getElementById("greskaPreplata").innerHTML = "Niste čekirati polje prijave!";
        return false;
    }
    return true;

}

function myFunction() {
    document.getElementById("myDropdown").classList.toggle("show");
    //document.getElementById("poz").style.color = "blue";
}


window.onclick = function (e) {
    if (!e.target.matches('.dropbtn')) {

        var dropdowns = document.getElementsByClassName("dropdown-content");
        for (var d = 0; d < dropdowns.length; d++) {
            var openDropdown = dropdowns[d];
            if (openDropdown.classList.contains('show')) {
                openDropdown.classList.remove('show');
            }
        }
    }
}

function showImage(smSrc, lgSrc) {
    document.getElementById('largeImg').src = smSrc;
    showLargeImagePanel();
    unselectAll();
    setTimeout(function () {
        document.getElementById('largeImg').src = lgSrc;
    }, 1)
}

function showLargeImagePanel() {
    document.getElementById('largeImgPanel').style.display = 'block';
}

function unselectAll() {
    if (document.selection)
        document.selection.empty();
    if (window.getSelection)
        window.getSelection().removeAllRanges();
}
window.onkeyup = function (event) {
    if (event.keyCode == 27) {
        document.getElementById('largeImgPanel').style.display = 'none';
    }
}