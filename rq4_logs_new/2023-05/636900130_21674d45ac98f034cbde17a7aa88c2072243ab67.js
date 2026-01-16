// receive beach name from POST from React app, 
// then send this name to mapbox API which will convert the name to lat/long. 
// This lat/long will be a JSON obj and you send this JSON obj back to the React app. 
// This API will be public on your github which will be linked to render.com for public use

import 'dotenv/config';
import express from 'express';
import axios from 'axios';

const PORT = 3000;
const app = express();
app.use(express.json());


//  GET CONTROLLER     ######################################
app.get('/get/:beachName', async (req, res) => {

    const beachName = req.params.beachName;           
    console.log("REQ BODY: " + beachName);

    const mapbox_token = 'pk.eyJ1IjoianlvanlvIiwiYSI6ImNsaGFvdnRieTBqZmgzcnMwbnZmZzZmcWoifQ.6AksXc5oklL4ZU-MPrkKWw';

    const url = 'https://api.mapbox.com/geocoding/v5/mapbox.places/' + `${beachName}` + '.json?access_token=' + `${mapbox_token}`;
    console.log(url);

    try {
        const response = await axios.get(url);
        res.json(response.data);
        console.log(response.data);

    } catch (error) {
        console.error(error);
    }

});


app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}...`);
});