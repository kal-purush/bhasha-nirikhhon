const theTextElem = document.getElementById("helloWorldTxt");

window.onload = function() {
    typeHelloWorld("Hello World!");
};

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function typeHelloWorldP1(time, theString, complete){
    const theTextElem = document.getElementById("helloWorldTxt");
    
    sleep(time).then(() => {
        var textStringRev = theTextElem.innerHTML.replace("|", "");
        theTextElem.innerHTML = textStringRev + theString + "|";
        if(complete){
            blinkingEffect(500, 1);
        }
    });
}

//"Hello World!" Typing Effect
function typeHelloWorld(theString){
    var isComplete = false;
    for(var i = 0; i < theString.length; i++){
        if(i == theString.length-1){
            isComplete = true;
        }
        typeHelloWorldP1(2500/theString.length * (i+1),theString.charAt(i), isComplete);
    }
}

function blinkingEffect(time, iteration){
    const theTextElem = document.getElementById("helloWorldTxt");
    const originalText = theTextElem.innerHTML.replace("|", "&nbsp");
    sleep(time).then(() => {
        theTextElem.innerHTML = originalText;
        sleep(time).then(() => {
            theTextElem.innerHTML = originalText.replace("&nbsp", "|");
            blinkingEffect(time*iteration, iteration++);
        })
    })
}