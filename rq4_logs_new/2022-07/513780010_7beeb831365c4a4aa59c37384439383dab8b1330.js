const cors = require('cors');
const path = require('path');
const bodyParser = require('body-parser');

const express = require('express');
const { application } = require('express');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));
app.use(express.json());
app.use(cors())

const PORT = 443;

app.get("/welcome/:name", function(req, res){
    const name = req.params.name;
    res.send("Welcome to my web server " + name);
});

//load game 1
app.get("/spacegame", function(req, res){
    app.use(express.static(__dirname+'/games/game1'))
    console.log(req.query.id)
    console.log(req.query.hostname)
    res.sendFile(__dirname + '/games/game1/game1.html')
});

//load game 2
app.use(express.static(__dirname+'/games/game2'))
app.get("/junglegame", function(req, res){
    res.sendFile(__dirname + '/games/game2/game2.html')
});

//load game 3
app.use(express.static(__dirname+'/games/game3'))
app.get("/homecoming", function(req, res){
    res.sendFile(__dirname + '/games/game3/index.html')
});

//load userfile
const defaultinfo = {
    "sites": [
    ],
    "skin": {
        "color":"#26257e",
        "theme":0
    }
}
app.use(express.static(__dirname+'/info'))
app.post("/info", function(req, res){
    console.log("info")
    let userid = req.body.userid;
    let fs = require('fs')
    if(fs.existsSync(`${__dirname}/info/${userid}_info.json`)){
        res.sendFile(`${__dirname}/info/${userid}_info.json`)
    }
    else{
        const fs = require('fs')
        fs.writeFile(`${__dirname}/info/${userid}_info.json`,JSON.stringify(defaultinfo),function(err){
            if (err === null) {
                console.log('success');
            } else {
                console.log('fail');
            }
        });
        res.sendFile(`${__dirname}/info/${userid}_info.json`)
    }
});

// save userfile
app.post("/infochange", function(req, res){
    const userid = req.body.userid;
    const info = req.body.data;
    console.log("infochange")
    console.log(req.body.data)
    const fs = require('fs')
    fs.writeFile(`${__dirname}/info/${userid}_info.json`, JSON.stringify(info), err => {
        if (err) console.log("Error writing file:", err);
    });
})

app.post("/welcome", function(req, res) {
    const name = req.body.name;
    res.send("Welcome " + name);
})

app.listen(PORT, function() {
    console.log("server is ready at " + PORT);
});


// reset user's current time, point, isbroken every day
const schedule = require('node-schedule');
const { dirname } = require('path');
const job = schedule.scheduleJob('* * 24 * * *', function(){
    resetInfo()
});

function resetInfo(){
    const infoFolder = `${__dirname}/info`
    let fs = require('fs')
    fs.readdir(infoFolder, function(err, filelist){
        if (err) throw err;
        filelist.forEach( file => {
            console.log(`${infoFolder}/${file}`)
            let rawdata = fs.readFileSync(`${infoFolder}/${file}`)
            let userinfo = JSON.parse(rawdata)
            userinfo.sites.forEach( (site, index) => {
                if(site.isbroken == 0){
                    userinfo.sites[index].point += 1
                }
                else{
                    userinfo.sites[index].point = 0
                }
                userinfo.sites[index].isbroken = 0  // reset is broken
                userinfo.sites[index].currenttime = 0 // reset time
            } )

            const fs1 = require('fs')
            fs1.writeFile(`${infoFolder}/${file}`, JSON.stringify(userinfo), err => {
                if (err) console.log("Error writing file:", err);
            });
        })

    })
}