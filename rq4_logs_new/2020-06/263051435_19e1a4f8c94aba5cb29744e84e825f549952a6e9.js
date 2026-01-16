
function turnOnOff(){
    var DenElement = document.getElementById("bubble");
    console.log(DenElement.src);
    if (DenElement.src == 'http://127.0.0.1:5500/off.gif'){
        console.log("bat");
        DenElement.src = 'on.gif';

    }
    else {
        console.log("tat");
        DenElement.src = 'off.gif';

    }
}
  
  