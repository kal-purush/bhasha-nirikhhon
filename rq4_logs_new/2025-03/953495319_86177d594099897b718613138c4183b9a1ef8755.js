const express = require("express");
const fs = require("fs");
const cors = require("cors");
const bodyParser = require("body-parser");

const app = express();
const PORT = 3000;

// Middleware
app.use(cors());  // Allows requests from frontend
app.use(bodyParser.json());  // Parses JSON data

// Route to handle contact form submissions
app.post("/saveContact", (req, res) => {
    const { name, email, message } = req.body;
    
    if (!name || !email || !message) {
        return res.status(400).json({ error: "All fields are required" });
    }

    const contactData = {
        name,
        email,
        message,
        timestamp: new Date().toLocaleString(),
    };

    const filePath = "contactData.json";

    // Read existing data or create an empty array
    let contacts = [];
    if (fs.existsSync(filePath)) {
        const existingData = fs.readFileSync(filePath);
        contacts = JSON.parse(existingData);
    }

    // Append new data
    contacts.push(contactData);
    
    // Save to JSON file
    fs.writeFileSync(filePath, JSON.stringify(contacts, null, 2));

    res.json({ success: "Data saved successfully!" });
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});