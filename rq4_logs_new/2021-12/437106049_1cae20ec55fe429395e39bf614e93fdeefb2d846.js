function closeBox()
{
    var box = document.getElementById("cookiebox");
    box.remove();
}

//there's probably a more elegant way to change the HTML than all these individual selectors
//but this works

function convertTemps(ele){
    console.log(`Convert to ${ele.value}`);

    var highTemp = [];
    var lowTemp = [];
    highTemp[0] = document.querySelector("#hightemp1").innerHTML;
    lowTemp[0] = document.querySelector("#lowtemp1").innerHTML;
    highTemp[1] = document.querySelector("#hightemp2").innerHTML;
    lowTemp[1] = document.querySelector("#lowtemp2").innerHTML;
    highTemp[2] = document.querySelector("#hightemp3").innerHTML;
    lowTemp[2] = document.querySelector("#lowtemp3").innerHTML;
    highTemp[3] = document.querySelector("#hightemp4").innerHTML;
    lowTemp[3] = document.querySelector("#lowtemp4").innerHTML;
    
    console.log(highTemp, lowTemp);

    for(var i = 0; i < 4; i++)
    {
        if(ele.value === 'C'){
            console.log("C");
            lowTemp[i] = (lowTemp[i] - 32)/ 1.8;
            highTemp[i] = (highTemp[i] -32)/1.8;
    }
        else if(ele.value === 'F'){
            console.log("F");
            highTemp[i] = (highTemp[i] * 1.8) + 32;
            lowTemp[i] = (lowTemp[i] * 1.8) + 32;
        }

    }
    console.log(highTemp, lowTemp);

//it's a mess if you don't round it

    document.querySelector("#hightemp1").innerHTML = Math.round(highTemp[0]);
    document.querySelector("#lowtemp1").innerHTML = Math.round(lowTemp[0]);
    document.querySelector("#hightemp2").innerHTML = Math.round(highTemp[1]);
    document.querySelector("#lowtemp2").innerHTML = Math.round(lowTemp[1]);
    document.querySelector("#hightemp3").innerHTML = Math.round(highTemp[2]);
    document.querySelector("#lowtemp3").innerHTML = Math.round(lowTemp[2]);
    document.querySelector("#hightemp4").innerHTML = Math.round(highTemp[3]);
    document.querySelector("#lowtemp4").innerHTML = Math.round(lowTemp[3]);
    
}


function alertme()
{
    alert('Loading weather report...');
}