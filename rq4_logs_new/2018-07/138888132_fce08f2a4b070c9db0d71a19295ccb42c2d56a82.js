const express = require('express');
const router = express.Router();
const axios = require('axios');

/** GET photo from google place photo api using the photo reference. */

router.get('/', function(req, res) {

    axios.get(`https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=${req.query.photoreference}&sensor=false&key=${process.env.API_KEY}`).
    
    then((response) => {

        res.send(response.request.res.responseUrl)

    }).
    catch((error) => {

        console.log(error);
    });
});

module.exports = router;