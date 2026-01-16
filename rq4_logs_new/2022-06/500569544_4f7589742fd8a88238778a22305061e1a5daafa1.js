var fs = require("fs")
var childproc = require("child_process")
const { stdout } = require("process")

var lol = childproc.execFile("./bin/rtl_adsb.exe")

lol.stdout.on("data", (data) => {
    parseInput(data)
})

function hexToBin(hex) {
    
}

var hexToDecTable = [
    "0000",
    "0001",
    "0010",
    "0011",
    "0100",
    "0101",
    "0110",
    "0111",
    "1000",
    "1001",
    "1010",
    "1011",
    "1100",
    "1101",
    "1110",
    "1111"
]

function parseInput(data){
    var butts = data.toString().replace("*", '').replace(";", '').trim()
    
    //console.log(butts)

    if (butts.toString().includes("*") || butts.toString().includes(";") || butts.toString().length > 28 || butts.toString().length < 28) {
        console.log("bad packet")
    } else {
        var adsb_signal_split = butts.toString().split("");
        //console.log(adsb_signal_split)
        
        var adsb_signal_binary = ""
        for (var i = 0; i < adsb_signal_split.length; i ++) {
            switch (adsb_signal_split[i]){
                case 0:
                    adsb_signal_binary += hexToDecTable[0];
                case 1:
                    adsb_signal_binary += hexToDecTable[1];
                case 2:
                    adsb_signal_binary += hexToDecTable[2];
                case 3:
                    adsb_signal_binary += hexToDecTable[3];
                case 4:
                    adsb_signal_binary += hexToDecTable[4];
                case 5:
                    adsb_signal_binary += hexToDecTable[5];
                case 6:
                    adsb_signal_binary += hexToDecTable[6];                    case 0:
                case 7:
                    adsb_signal_binary += hexToDecTable[7];
                case 8:
                    adsb_signal_binary += hexToDecTable[8];
                case 9:
                    adsb_signal_binary += hexToDecTable[9];
                case "a":
                    adsb_signal_binary += hexToDecTable[10];
                case "b":
                    adsb_signal_binary += hexToDecTable[11];
                case "c":
                    adsb_signal_binary += hexToDecTable[12];
                case "d":
                    adsb_signal_binary += hexToDecTable[13];
                case "e":
                    adsb_signal_binary += hexToDecTable[14];
                case "f":
                    adsb_signal_binary += hexToDecTable[15];
            }
            
        }
        console.log(adsb_signal_binary)
        //console.log(butts.toString().split()
    }
    // console.log(data.toString().slice(1, data.toString().length - 3))
    //butts = data.toString().replace();
    //console.log(data)
}