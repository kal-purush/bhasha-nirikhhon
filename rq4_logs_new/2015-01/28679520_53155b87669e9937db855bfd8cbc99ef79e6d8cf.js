window.onload = function(){
  var running = false;
  var interval;
  var decimal = 0;
  var sec = 0;
  var min = 0;
  var cs = 0;
  var decimalOut = document.getElementById("decimal");
  var secOut = document.getElementById("sec");
  var minOut = document.getElementById("min");
  var colon = document.getElementById("colon");
  var timesOut = document.getElementById("timeOut");
  var timesList = document.getElementById("timeList");
  var clearAll = document.getElementById("clear");
  var timesDisplay = new Array();
  var csTimes = new Array();
  var avAll = 0;
  var avAllOut = document.getElementById("overallAv");
  var best = Number.MAX_VALUE;
  var bestOut = document.getElementById("fastest");
  var worst = 0;
  var numSolves = 0;
  var total = 0;
  var numSolvesOut = document.getElementById("solveNum");
  generateScramble();
  function timer(){
    decimal++;
    cs++; //counts time in centiseconds
    decimalOut.innerHTML = decimal;
    if(decimal>=100){
      decimal = 0;
      sec++;

      if(sec>59){
        sec = 0;
        min++;
        colon.innerHTML = ":";
        minOut.innerHTML = min;
      }
      if(sec<=9 && min>0){
        sec = "0"+sec;
      }
      secOut.innerHTML = sec;
    }

    if(decimal<=9){
      decimal = '0'+decimal;
      decimalOut.innerHTML = decimal;
    }

  }
  window.onkeyup = run;
  function run(){
    if(!running){
      decimal=0;
      sec=0;
      min=0;
      cs=0;
      secOut.innerHTML = "0";
      minOut.innerHTML = "";
      colon.innerHTML = "";
      running = true;
      scramble = "";
      generateScramble();
      interval = setInterval(timer,10);
    }
    else if(running){
      running = false;
      clearInterval(interval);
      timesDisplay.push(" "+timesOut.innerHTML);
      csTimes.push(cs);
      timesList.innerHTML = timesDisplay;
      calculateStats();
    }
  }
  function generateScramble(){
    var move;//includes face to turn and how to turn it. Ex. 2F
    var face;//Face to turn. Either R, L, F, B, U, or D
    var faceNum;//1-6, corresponds to face R-D
    var lastFaceNum = 10;//The face of the previous turn
    var turn;//How to turn a face. Either ', 2, or nothing.
    var scramble = "";//inlucdes 25 moves
    var output = document.getElementById("scram");
    for(var x = 0; x<25; x++)
    {
      do{
        faceNum = Math.floor(Math.random()*6)+1;
      }while(faceNum === lastFaceNum);//the same face can't appear in consecutive moves.
      lastFaceNum = faceNum;
      if(faceNum === 1) {face = "R";}
      if(faceNum === 2) {face = "L";}
      if(faceNum === 3) {face = "U";}
      if(faceNum === 4) {face = "D";}
      if(faceNum === 5) {face = "F";}
      if(faceNum === 6) {face = "B";}
      turn = Math.floor(Math.random()*3)+1;
      if(turn === 1){move = face;}
      if(turn === 2){move = face+"2";}
      if(turn === 3){move = face+"'";}

      scramble += move+" ";
    }
    output.innerHTML = scramble;
  }
  clearAll.onclicked = clearTimes;
  clearAll.addEventListener('click', function() {
          clearTimes();
      });
  function clearTimes(){
    numSolves = 0;
    numSolvesOut.innerHTML = "Solves: "+numSolves;
    /*av5 = 0;
    av5Out.innerHTML = "3/5: ";
    av12 = 0;
    av12Out.innerHTML = "10/12: ";*/
    best = 99999999999;
    bestOut.innerHTML = "Best: ";
    worst = 0;
    avAll = 0;
    total = 0;
    avAllOut.innerHTML = "Average: ";
    timesDisplay = [];
    csTimes = [];
    timesList.innerHTML = timesDisplay;
  }
  function calculateStats(){
    numSolves++;
    total = 0;
    numSolvesOut.innerHTML = "Solves: "+numSolves;
    for(var x = 0; x<csTimes.length; x++){
      if(csTimes[x]<best){
        best = csTimes[x];
      }
      if(csTimes[x]>worst){
        worst = csTimes[x];
      }
      total += csTimes[x];
    }
    avAll = total/numSolves;
    avAllOut.innerHTML = "Average: "+formatTime(avAll);
    bestOut.innerHTML = "Best: "+formatTime(best);
  }
  function formatTime(t){
    //m = minute, s = second, c = centisecond
    var m = 0, s = 0, c = 0, out = "";
    m = Math.floor(t/6000);
    t = t%6000;
    s = Math.floor(t/100);
    t = t%100;
    c = Math.floor(t);
    if(m<1){
      m="";
    }
    else{
      m = m+":";
      if(s<10){
        s = "0"+s;
      }
    }
    if(c<10){
      c = "0"+c;
    }

    out = ""+m+s+"."+c;
    return out;
  }
}