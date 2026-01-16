import express from "express";
import axios from "axios";
import cors from "cors";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3002;
const API_KEY = process.env.GOOGLE_API_KEY;
console.log(API_KEY, "apiiiiiiiiiiii keyyyyyyyy");

app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, "../client/build")));

app.get("/api/search", async (req, res) => {
  const query = req.query.q || "";

  try {
    const response = await fetch(
      `http://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=${query}`,
      {
        method: "GET",
      }
    );

    if (!response.ok) {
      throw new Error("Network response was not ok");
    }

    const data = await response.json();
    // console.log(data, "search response");

    res.json(data);
  } catch (error) {
    console.error("Error fetching search suggestions:", error.message);
    res.status(500).send("Error fetching search suggestions");
  }
});

app.get("/api/videos", async (req, res) => {
  console.log(API_KEY, "apiiiiiiiiiiii keyyyyyyyy");

  try {
    const response = await axios.get(
      "https://youtube.googleapis.com/youtube/v3/videos",
      {
        params: {
          part: "snippet,contentDetails,statistics",
          chart: "mostPopular",
          maxResults: 50,
          regionCode: "IN",
          key: API_KEY,
        },
      }
    );
    // console.log(response.data, "response");

    res.json(response.data);
  } catch (error) {
    console.error("Error fetching videos:", error.message);
    res.status(500).send("Error fetching data from YouTube API");
  }
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "../build", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});