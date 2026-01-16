const express = require("express");
const path = require("path");
const fs = require("fs");
const cors = require("cors");
require('dotenv').config();
const fetch = require('node-fetch'); 
const axios = require("axios");
const bodyParser = require('body-parser');


const { exec } = require("child_process");

const url = "https://futaa.onrender.com";
exec(`start chrome --app="${url}"`);


const app = express();
const PORT = process.env.PORT || 3000;

const { GoogleGenerativeAI } = require("@google/generative-ai");
const { log } = require("console");
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });
app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, "public")));
app.use(bodyParser.json());


app.post('/save-geojson', (req, res) => {
    const geojson = req.body.geojson;
    if (!geojson) {
      return res.status(400).json({ error: 'No geojson data received' });
    }
  
    const savePath = path.join(__dirname, 'public', 'assets', 'geojson', 'geojsonfile.json');
  
    fs.writeFile(savePath, JSON.stringify(geojson, null, 2), err => {
      if (err) {
        console.error('Error saving geojson:', err);
        return res.status(500).json({ error: 'Failed to save geojson' });
      }
  
      return res.json({ message: 'GeoJSON saved successfully' });
    });
  });



  const overpassEndpoints = [
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter"
  ];
  
  app.post("/get-osm-data", async (req, res) => {
    const { geojson, featureType } = req.body;
    console.log("Received GeoJSON:", geojson);
  
    // Convert GeoJSON to Overpass (lat lon) format
    const coordinates = geojson.geometry.coordinates[0]
      .map(coord => `${coord[1]} ${coord[0]}`) // lat lon
      .join(" ");
    
    console.log("Converted Coordinates (lat, lon):", coordinates);
  
    // Build Overpass Query
    const overpassQuery = `
      [out:json][timeout:25];
      (
        node["${featureType}"](poly:"${coordinates}");
        way["${featureType}"](poly:"${coordinates}");
        relation["${featureType}"](poly:"${coordinates}");
      );
      out body;
      >;
      out skel qt;
    `;
  
    const requestBody = `data=${encodeURIComponent(overpassQuery)}`;
  
    // Try each Overpass server in order
    for (const url of overpassEndpoints) {
      try {
        console.log(`Trying Overpass API: ${url}`);
        const response = await axios.post(url, requestBody, {
          headers: {
            "Content-Type": "application/x-www-form-urlencoded"
          },
          timeout: 25000
        });
  
        console.log(`Overpass API succeeded at: ${url}`);
        return res.json(response.data);
      } catch (error) {
        console.warn(`Failed at ${url}: ${error.message}`);
      }
    }
  
    console.error("All Overpass API endpoints failed.");
    res.status(500).send("Failed to fetch OSM data from all Overpass endpoints.");
  });




// app.get("/ask-ai", async (req, res) => {
//     const userQuestion = req.query.message.toLowerCase();  // Get the question from frontend

//     // const prompt = "You are a mapping assistant.";

//     try {
//         const response = await model.generateText({
//             prompt: userQuestion,
//             temperature: 0.5,
//             maxOutputTokens: 100,
//             topP: 0.8,
//             topK: 40,
//         });
//         console.log(response);
//         res.json({ answer: response.text });
//     } catch (error) {
//         console.error("Error generating response:", error);
//         res.status(500).json({ error: "Failed to generate response" });
//     }

// });






app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});