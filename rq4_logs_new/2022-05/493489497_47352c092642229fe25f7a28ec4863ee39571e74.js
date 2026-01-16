var BtnDark = document.querySelector("#dark");
var BtnLight = document.querySelector("#light");
BtnDark.onclick = DarkMode;
BtnLight.onclick = LightMode;
function DarkMode(){
    document.querySelector("body").style.color="white"
    document.querySelector("body").style.backgroundColor="black"
}
function LightMode(){
    document.querySelector("body").style.color="black"
    document.querySelector("body").style.backgroundColor="white"
}