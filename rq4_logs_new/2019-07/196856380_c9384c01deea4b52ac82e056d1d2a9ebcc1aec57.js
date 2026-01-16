'use strict';
const BaseballImage = require("baseballimage");

// app.route('/baseball/image/team/:team')
exports.getImage = async (req, res) => {
    // const weatherConfig = {
    //     team
    //     lat: req.params.lat,            // lat: "41.7476",
    //     lon: req.params.lon,            // lon: "-70.6676",
    //     agent: "ken@faubel.org",
    //     title: req.params.title         // "Forecast for Onset, MA"
    // }

    // team = req.params.team;

    console.log("BaseballController: " + req.params.team); 

    const baseballImage = new BaseballImage();

    const imageStream = await baseballImage.getImageStream(req.params.team);

    if (imageStream === null) {
        res.send("Uanble to retreive image.  Something went wrong");
        return;
    }

    imageStream.pipe(res);
};