function openSondage() {
    document.getElementById("sondage").classList.toggle('fade');
    document.getElementById("openbtn").setAttribute('onclick', "closeSondage()");
    document.getElementById("openbtn").style.backgroundColor = "#ff0000";
}

function closeSondage() {
    document.getElementById("sondage").classList.toggle('fade');
    document.getElementById("openbtn").setAttribute('onclick', "openSondage()");
    document.getElementById("openbtn").style.backgroundColor = "#00dd00";
}