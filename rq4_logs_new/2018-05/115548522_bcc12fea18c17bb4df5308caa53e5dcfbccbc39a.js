$(document).ready(function(){$("body").hide().fadeIn(1000);});

document.getElementById("portfolio_link").addEventListener("click", showPort);
document.getElementById("pclose").addEventListener("click", closePort);

document.getElementById("about_link").addEventListener("click", showAbout);
document.getElementById("cclose").addEventListener("click", closePort);

function showAbout() {
    var aboutDiv = document.getElementById("about");
    var sideDiv = document.getElementById("sidebar");
    var portDiv = document.getElementById("portfolio");
    var aboutOp = aboutDiv.style.opacity;
    if (aboutOp == 0) {
        portDiv.style.opacity = 0;
        aboutDiv.style.opacity = 1;
        sideDiv.style.marginLeft = "-350px";
        sideDiv.style.opacity = .4;
        aboutDiv.style.display = "block";
    } else {
        aboutDiv.style.opacity = 0;
        sideDiv.style.marginLeft = "-170px";
        sideDiv.style.opacity = 1;
    }
}


function showPort() {
    var portDiv = document.getElementById("portfolio");
    var sideDiv = document.getElementById("sidebar");
    var aboutDiv = document.getElementById("about");
    var portOp = portDiv.style.opacity;
    
    if (portOp == 0) {
        aboutDiv.style.opacity = 0;
        portDiv.style.opacity = 1;
        sideDiv.style.marginLeft = "0px";
        sideDiv.style.opacity = .4;
    } else {
        sideDiv.style.opacity = 1;
        sideDiv.style.marginLeft = "-170px";
        portDiv.style.opacity = 0;
    }
}

function closePort() {
    var portDiv = document.getElementById("portfolio");
    var aboutDiv = document.getElementById("about");
    var sideDiv = document.getElementById("sidebar");
    aboutDiv.style.opacity = 0;
    portDiv.style.opacity = 0;
    sideDiv.style.opacity = 1;
    sideDiv.style.marginLeft = "-170px";
}