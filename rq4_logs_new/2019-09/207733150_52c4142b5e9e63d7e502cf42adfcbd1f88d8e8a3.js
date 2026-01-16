function RandomInt(ll, hl){
    return Math.floor(Math.random() * (hl - ll + 1)) + ll;
}
function GenerateMeteringData(ll, hl, count, analogOrDiscrete){
    var roundingAccuracy;
    analogOrDiscrete === "a" ? roundingAccuracy = 2 : roundingAccuracy = 0; 

    var vals = [];
    var avg = (hl + ll) / 2;
    var pointsPerCycle = 10 * RandomInt(3,1);
    var fullCycles = Math.floor(count / pointsPerCycle);
    var tail = count % pointsPerCycle;

    for(var i = 0; i < fullCycles ; i++){
        var intensityLevel = RandomInt(1,2);
        var firstValueOfTail;
        var val;
        for(var j = 0; j < pointsPerCycle ; j++){

            j === 0 ?  val = avg : val = avg + (hl - avg) / intensityLevel * Math.sin((j / pointsPerCycle) * 2 * Math.PI);
            vals.push(Number(val.toFixed(roundingAccuracy)));
            i === fullCycles - 1 ? firstValueOfTail = val : firstValueOfTail = null;
        }

        if(i == fullCycles - 1){
            firstValueOfTail = val;
            for(var k = 0; k < tail ; k++){
                var val;
                k === 0 ?  val = firstValueOfTail : val = firstValueOfTail + (hl - firstValueOfTail) / intensityLevel * Math.sin((k / pointsPerCycle) * 2 * Math.PI);
                vals.push(Number(val.toFixed(roundingAccuracy)));
            }

        }
    }

    return vals;
}

var data = GenerateMeteringData(0, 100, 150);