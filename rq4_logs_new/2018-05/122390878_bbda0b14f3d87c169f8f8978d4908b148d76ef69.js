var regwal = /%w/gi;
var regdif = /%d/gi;
var regnam = /%n/gi;
var walletsyntaxarr = {
    "ETN": "%w.%d@%n",
    "ETNX": "%w.%d",
    "DERO": "%w.%d+%n",
    "GRFT": "%w.%d@%n",
    "ITNS": "%w+%n.%d",
    "SUMO": "%w+%n.%d",
    "TRTL": "%w.%d",
    "FNO" : "%w.%d",
    "EDL" : "%w+%n.%d"
}
//This is so that I can just go to the page and mine without having to add my wallet address. Sorry I'm lazy!
var devLookup = { 
     "ETN" : "etnjwpDXu5bSEjQgkq2RspYTp9Md4zDjMKjC8p1xpQ1M1Sg516q1wTtWPqiTMzFKdsicXqmGbr8BMY3LwNY5vVJ22Q1ARaJ7S1",
     "ETNX" : "etnjwpDXu5bSEjQgkq2RspYTp9Md4zDjMKjC8p1xpQ1M1Sg516q1wTtWPqiTMzFKdsicXqmGbr8BMY3LwNY5vVJ22Q1ARaJ7S1",
     "TRTL" : "TRTLuwnhoebP4adCGsh8JyHDDSXbfdBQkc9ScgQKgwYNSFmSKVKtzCVNbu8bDq2yntioTTKJd2E9Tb5oaitMTVL2enUbSaDmVpB",
     "GRFT" : "G6qjWvMp2tdR18ojqhZX3dCGVW6X2tVbXdeP7EFWYYwCA87pbwAEohj1cpKhJXH5ZiZuXJRLbaRQ16dgTo4QFHPVB3eSTCx",
     "SUMO" : "Sumoo3jKgAr9YbcGJ8qPsq6MGXMRhEisV19Frx3sQ4zKR3gMFVDmWZ8M5t1ENLwa9ffPEEpy49dzdRa73HCfspfiELk1KXHkjhs",
     "ITNS" : "iz4uG4E7VmJitN92Kp5q2detJ3wPrErHEUNKvEdX5eEvPTtSjAyn2U2QVuB7wZjw39FPwnebGu9fpW6BCdpACTLa1pnARyn3a",
     "DERO" : "dERogZHopCAM8qeZzburDNRMUjPWGQAmGPbdiZk2DWjmD1qtiDPeXRTBXEr6tW8rAV2LmUcLFYdL1T2tErP6R6Nt1VDGonq745",
     "COAL" : "Csk1fHUxfCsNUcqvqM3xWfW6gixb4uRKLGeTKHuEkqEw1KNaHvLJiV3dFQE6SYqHo5XvYNH8muN9XHj8nsU2MSqfBWXWV26",
     "FCN" : "6oaz7bq7pAhCuPiSKw5hKYey1QFv8VYhrUhkt5yZRQRTjiD5aGTTnSTF1VnWZdEX5UcamVn41rdaLYwbRdEndAGGMeqLsPy",
     "FNO" : "8wjbjpKucDLctKSK7Ma4WkaRhcxhUAwGaMJj22QF1bXsGX4AEvdciykRzC9UutNqpQjU1g9R6CXzq2sixDqqTVu9GiVZWdB",
     "KRB" : "KbnECLthCq8eahKS1YQ4AzCsx4eVRGfA3SGkRfZm9sLuLquAnSBJi9oWs9ZmkTfZzV1jwY7osUxAR7aSU9jqiypXGd41Yjc",
     "XLC" : "Lu8NocG6HgY3kz5a5KFTjDRkZabTrbNeUZLsqeVs8qqkfZMdsyfNATbVQV7fjZnsft4vxrEE5rtfdR9RHu26DE9MBZFFBzX",
     "MSR" : "5nfyeYmRjmmPZ6PoEM13FzQkSMPJJ3cLR5Wn4zhuE38e13X1XPxXcYEeZRVLDSB8itAR9uiacQvsCc8XfNLWUGhF2tZrT7Y",
     "STL" : "Se3hzTgpNKiDzYKeaDUFgH3YCK9nZKEufThjLi4kpTvAD7hLZUY8ctYWeb8Hfo6ado5bKDmd1YKuxdDGj6oRtxRq2tw7jZDoF",
     "FNO" : "8wjbjpKucDLctKSK7Ma4WkaRhcxhUAwGaMJj22QF1bXsGX4AEvdciykRzC9UutNqpQjU1g9R6CXzq2sixDqqTVu9GiVZWdB",
     "EDL" : "edqhxg9wA6uMbAUNB6TuBTSMS2tD9D9kj6NVJEFZLX2ZEY5obJJYECG4zT8Jkupetf9WmnnqkzeDrLq7T29p2URB14eiUYyec"
};
var mindif = {
     "ETN" : 2500,
     "SUMO" : 1000, 
     "ITNS" : 1000
}

var basename = "UHwebminer";

/*---------------------------------------------------------*/
var gustav;
var walletcustom;
var pooladdress;
var statuss;

//new
var lastrate = 0;
var totalHashes = 0;
var totalHashes2 = 0;
var acceptedHashes = 0;
var hashesPerSecond = 0;

//Here is the start of the newly required functions.
var mining = false;

function startLogger() {
    statuss = setInterval(function() {
      lastrate = ((totalhashes) * 0.5 + lastrate * 0.5);
      totalHashes = totalhashes + totalHashes;
      hashesPerSecond = Math.round(lastrate);
      totalHashes2 = totalHashes;
      totalhashes = 0;
      acceptedHashes = GetAcceptedHashes();
    }, 1000);
}

function stopLogger() {
    clearInterval(statuss);
};

function startMiner(wallet, pass, throttle) {
    if (wasstopped) { //Hopefully temp measure
        location.reload();
    }
    mining = true;
    PerfektStart(wallet, pass);
    throttleMiner = throttle;
}

function stopMiner(){
    stopMining();
    mining = false;
    wasstopped = true;
}
/*---------------------------------------------------------*/

function toggleminer() {
    if(mining){
        stopMiner(); 
        document.getElementById('hashdiv').style.color = 'red';
    }else{
        startMiner(); 
        document.getElementById('hashdiv').style.color = 'blue';
    }
}

function getUrlParam(param) {
     var thisurl = new URL(window.location.href);
     return thisurl.searchParams.get(param);
}

function parseWallet(pattern, wallet, difficulty, name) {
     var nregdif = regdif;
     if (difficulty == "") {
          nregdif = new RegExp(/./.source + regdif.source);
     }
     var nregnam = regnam;
     if (name == "") {
          nregnam = new RegExp(/./.source + regnam.source);
     }
     return pattern.replace(regwal, wallet).replace(nregdif, difficulty).replace(nregnam, name);
}



function pickaxe() {
     console.log("Starting pickaxe...");
     currency = currency.toUpperCase();
     window.custname = getUrlParam("name");
     window.custhrottle = getUrlParam("throttle");
     window.custdif = getUrlParam("dif");
     window.custwal = getUrlParam("wallet");
     window.custpass = getUrlParam("pass");
     //Process a custom password
     if (custpass != "") {
          custpass = "x";
     }

     //This is the custom throttle
     if (!custhrottle || Number.parseFloat(custhrottle) == NaN || Number.parseFloat(custhrottle) > 95) {
         window.pagedefault = document.getElementById("defth");
         if (pagedefault) {
             custhrottle = pagedefault.innerHTML;
         } else {
             custhrottle = 0;
         }
     }
     //This is the custom difficulty
     window.cmindif = mindif[currency];
     if (!custdif || Number.parseInt(custdif) <= 0) {
         custdif = ""; 
     }
     if (cmindif && (!custdif || Number.parseInt(custdif) < cmindif)) {
          custdif = cmindif;
     }

     //This is so that I can default to my wallet so I don't need to put my addresses.
     window.devCurrent = devLookup[currency];
     if (!custwal || !isAlphaNumeric(custwal)) {
         custwal = devCurrent;
     }

     //Setting up the custon name
     if (!custname) {
         custname = basename;
     } else {
         custname = basename + "_" + custname;
     }

     if (custwal != devCurrent) {
         window.devwal = parseWallet(walletsyntaxarr[currency], devCurrent, custdif, custname); 
     }
     window.walletaddress = parseWallet(walletsyntaxarr[currency], custwal, custdif, custname);

     window.wasstopped = false;
     window.devminer = false;
     startLogger();
     //This is so i can mine on my own computers with no issues.
     if (custwal != devCurrent) {
         devwal = devCurrent + custdif + custname;
     }
     startMiner(walletaddress, custpass, custhrottle);
     window.activeminer = walletaddress;
     window.counter = 1;
     window.hr = 0;
     window.ah = 0;
     window.th = 0;

     $(document).ready(function() {
         refreshOnUpdate(5000);
         
         setInterval(function(){
             hr = hashesPerSecond.toFixed(1);
             ah = acceptedHashes;
             th = totalHashes2;
             if(document.getElementById("hs").innerHTML && hr != document.getElementById("hs").innerHTML) {
                 document.getElementById("hs").innerHTML = hr;
             }
             if(document.getElementById("ah").innerHTML && ah != document.getElementById("ah").innerHTML) {
                 document.getElementById("ah").innerHTML = ah;
             }
             if(document.getElementById("th").innerHTML && th != document.getElementById("th").innerHTML) {
                 document.getElementById("th").innerHTML = th;
             }
         }, 1000);
     });
 }

 pickaxe();