const express = require("express");
const cors = require("cors");
const axios = require("axios");
const dotenv = require("dotenv");

dotenv.config();

const app = express();


app.use(express.json());
app.use(cors());

const JUDGE0_API_URL = "https://judge0-ce.p.rapidapi.com/submissions"; // ✅ Fixed Syntax Error
const API_KEY = process.env.JUDGE0_API_KEY;
app.get("/", (req, res) => {
  res.send("🚀 Code Execution API is running!");
});


app.post("/api/run", async (req, res) => {
  console.log("Received Request Body:", req.body); // ✅ Debugging: Check if frontend sends correct data

  const { language_id, source_code, stdin } = req.body;

  if (!language_id || !source_code) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const response = await axios.post(
      `${JUDGE0_API_URL}?base64_encoded=false&wait=true`,
      { source_code, language_id, stdin },
      {
        headers: {
            "Content-Type": "application/json",
            "X-RapidAPI-Key": API_KEY,  // 🔴 API key check karo
            "X-RapidAPI-Host": "judge0-ce.p.rapidapi.com",
        }
      }
    );

    res.json(response.data);
  } catch (error) {
    console.error("Judge0 API Error:", error);
    if (error.response) {
      res.status(error.response.status).json({ error: error.response.data });
    } else {
      res.status(500).json({ error: "Code execution failed" });
    }
  }
});

const PORT = process.env.PORT || 10000; // ✅ Render assigns a port
app.listen(PORT, "0.0.0.0", () => console.log(`🚀 Server running on port ${PORT}`));


