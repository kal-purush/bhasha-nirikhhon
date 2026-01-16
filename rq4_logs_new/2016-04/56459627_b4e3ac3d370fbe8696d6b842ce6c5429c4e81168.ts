
var context = new AudioContext();
//var osc = new OscillatorNode();
var gainNode, source, destination,osc,filterNode,convolverNode;
//gainNode.gain.value = 0.4;
//var mic = context.createMediaStreamSource();
var analyser;
analyser.fftSize = 2048;
var fFrequencyData = new Float32Array(analyser.frequencyBinCount);
var bFrequencyData = new Uint8Array(analyser.frequencyBinCount);
var bTimeData = new Uint8Array(analyser.frequencyBinCount);

    function play() {
        osc = context.createOscillator();
        analyser = context.createAnalyser();
        gainNode = context.createGain();
        filterNode = context.createBiquadFilter();
        //filterNode.frequency = 3000;
        osc.connect(filterNode);
        osc.connect(analyser);
        osc.connect(gainNode);
        gainNode.connect(context.destination);
        analyser.connect(context.destination);
        filterNode.connect(context.destination);
        //osc.connect(context.destination);
        osc.start();
        printAnalyser();
    }

    function printAnalyser(){
        var v1 = document.getElementById('text1');
        

        this.getElementById("text2").innerHTML(analyser.getByteFrequencyData(bFrequencyData));
        this.getElementById("text3").innerHTML(analyser.getByteTimeDomainData(bTimeData));
    }

  function stop() {
        osc.stop();
		location.reload()
    }
function changeParam(paramName, target) {
    osc[paramName].value = target.value;
//    console.log(osc[paramName].name + ':' + target.value);
}

function changeGain(target){
    gainNode.gain.value = target.value;
}

function changeFilter(target) {
    if(target.value==1)
    {
        filterNode.type = 1;
        filterNode.frequency.value = 1000;
        filterNode.frequency.Q = 1;
    }
    else if(target.value==2)
    {
        filterNode.type = 2;
        filterNode.frequency.value = 150;
        filterNode.frequency.Q = 10;
    }
}
function changeFilter2(target){
    if(target.value==1)
    {
        filterNode.type = 1;
        filterNode.frequency.value = 1000;
        filterNode.frequency.Q = 1;
    }
    else if(target.value==2)
    {
        filterNode.type = 2;
        filterNode.frequency.value = 150;
        filterNode.frequency.Q = 10;
    }
    else filterNode.frequency.value = 0;
}

/*class Greeter {
    element: HTMLElement;
    span: HTMLElement;
    timerToken: number;

    constructor(element: HTMLElement) {
        this.element = element;
        this.element.innerHTML += "The time is: ";
        this.span = document.createElement('span');
        this.element.appendChild(this.span);
        this.span.innerText = new Date().toUTCString();
    }

    start() {
        this.timerToken = setInterval(() => this.span.innerHTML = new Date().toUTCString(), 500);
    }

    stop() {
        clearTimeout(this.timerToken);
		location.reload()
    }

}

window.onload = () => {
    var el = document.getElementById('content');
    var greeter = new Greeter(el);
    greeter.start();
};*/