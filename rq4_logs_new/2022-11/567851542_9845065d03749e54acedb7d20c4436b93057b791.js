var emmanuellaHeight = 10;
var stanleyHeight = 15;
var emmanuellaMass = 100;
var stanleyMass = 90;
var stanleyBMI = stanleyMass/(stanleyHeight*stanleyHeight);
var emmanuellaBMI = emmanuellaMass/(emmanuellaHeight*emmanuellaHeight);
var myBMI;
if (stanleyBMI>emmanuellaBMI){
    console.log(`"Stanley's BMI (${stanleyBMI}) is higher than Emmanuella's (${emmanuellaBMI})!"`)
}
else {
    console.log(`"Emmanuella's BMI (${emmanuellaBMI}) is higher than Stanley's (${stanleyBMI})!"`)
}