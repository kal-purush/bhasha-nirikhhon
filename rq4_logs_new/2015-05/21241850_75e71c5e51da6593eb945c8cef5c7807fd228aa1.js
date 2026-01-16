/**
 * Created by olivier on 01/08/14.
 */
function changerfond(numfond){
    switch(numfond){
        case 1:document.getElementById('divgauche').style.backgroundColor = "blue";break;
        case 2:document.getElementById('divmilieu').style.backgroundColor = "blue";break;
        case 3:document.getElementById('divdroite').style.backgroundColor = "blue";break;
    }
}

function annulerfond(numfond){
    switch(numfond){
        case 1:document.getElementById('divgauche').style.backgroundColor = "black";break;
        case 2:document.getElementById('divmilieu').style.backgroundColor = "black";break;
        case 3:document.getElementById('divdroite').style.backgroundColor = "black";break;
    }
}