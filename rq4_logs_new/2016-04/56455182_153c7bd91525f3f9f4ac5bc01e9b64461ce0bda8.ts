
var context = new AudioContext();
//var osc = new OscillatorNode();
var gainNode, source, destination, osc, filterNode, convolverNode;
//gainNode.gain.value = 0.4;
//var mic = context.createMediaStreamSource();

var analyser;

analyser = context.createAnalyser();

analyser.fftSize = 64;
var fFrequencyData = new Float32Array(analyser.frequencyBinCount);
var bFrequencyData = new Uint8Array(analyser.frequencyBinCount);
var bTimeData = new Uint8Array(analyser.frequencyBinCount);


function play() {
    osc = context.createOscillator();
    gainNode = context.createGain();
    filterNode = context.createBiquadFilter();
    osc.connect(filterNode);
    osc.connect(analyser);
    osc.connect(gainNode);
    gainNode.connect(context.destination);
    analyser.connect(context.destination);
    filterNode.connect(context.destination);
    osc.start();
    
    print();
}


   

function print() {

    // Получаем данные
    analyser.getFloatFrequencyData(fFrequencyData);
    analyser.getByteFrequencyData(bFrequencyData);
    analyser.getByteTimeDomainData(bTimeData);

    
    var l1, l2, l3;
    l1 = document.getElementById('arr1');
    l2 = document.getElementById('arr2');
    l3 = document.getElementById('arr3');

    l1.textContent = "1";
    l2.textContent = "2";
    l3.textContent = "3";

    l1.textContent = fFrequencyData.toString();
    l2.textContent = bFrequencyData.toString(); 
    l3.textContent = bTimeData.toString();

  
}

function stop() {
    osc.stop();
}
function changeParam(paramName, target) {
    osc[paramName].value = target.value;

    print();

}

function changeGain(target) {
    gainNode.gain.value = target.value;
}

function changeFilter(target) {
    if (target.value == 1) {
        filterNode.type = 1;
        filterNode.frequency.value = 1000;
        filterNode.frequency.Q = 1;
    }
    else if (target.value == 2) {
        filterNode.type = 2;
        filterNode.frequency.value = 150;
        filterNode.frequency.Q = 10;
    }
}
function changeFilter2(target) {
    if (target.value == 1) {
        filterNode.type = 1;
        filterNode.frequency.value = 1000;
        filterNode.frequency.Q = 1;
    }
    else if (target.value == 2) {
        filterNode.type = 2;
        filterNode.frequency.value = 150;
        filterNode.frequency.Q = 10;
    }
    else filterNode.frequency.value = 0;
}
