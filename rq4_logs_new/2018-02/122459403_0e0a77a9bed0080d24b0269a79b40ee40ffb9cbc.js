/**********************************
 * @author      Wouter Vlaeyen
 * @created     22/02/2018
 * @modified    22/02/2018
 * @copyright   Copyright © 2016-2017 Artevelde University College Ghent
 * @function    Week 2
 **********************************/

let calculate = function() {
    let getal1 = document.getElementById('getal1').value;
    let getal2 = document.getElementById('getal2').value;
    
    getal1 = parseInt(getal1);
    getal2 = parseInt(getal2);

    let res;

    if(getal1 > getal2) {
        res = getal1 + " is het grootste getal.";
    }
    else {
        res = getal2 + " is het grootste getal.";
    }

    document.getElementById('result').innerHTML = res;
}

 // Add event listener
document.getElementById('calcbtn').addEventListener('click', calculate);