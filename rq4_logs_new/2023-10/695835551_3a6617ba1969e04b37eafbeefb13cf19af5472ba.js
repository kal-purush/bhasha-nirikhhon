const express = require("express");
const bcrypt = require("bcrypt");
const { Student, Mentor, Admin } = require("../models/model3"); // Import your models

const router = express.Router();

// Define your login route
router.post("/", async (req, res) => {
  try {
    const { username, password, userType } = req.body;

    let user;

    // Depending on the userType, find the user in the appropriate collection
    if (userType === "student") {
      user = await Student.findOne({ username });
    } else if (userType === "mentor") {
      user = await Mentor.findOne({ username });
    } else if (userType === "admin") {
      user = await Admin.findOne({ username });
    } else {
      return res.status(400).json({ message: "Invalid user type" });
    }

    // Check if the user exists
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    // Compare the provided password with the stored hashed password
    const passwordMatch = await bcrypt.compare(password, user.password);

    if (!passwordMatch) {
      return res.status(401).json({ message: "Incorrect password" });
    }

    // At this point, the login is successful
    // You can set a session or return a success message as per your requirement
    return res.status(200).json({ message: "Login successful" });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;