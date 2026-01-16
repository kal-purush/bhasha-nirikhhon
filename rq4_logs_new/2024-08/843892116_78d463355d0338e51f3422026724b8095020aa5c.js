const fs = require("fs");
const path = require("path");
const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const cors = require("cors");

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

app.use(
  cors({
    origin: "*", // Allow all origins
    methods: ["GET", "POST", "PUT", "DELETE"], // Allow specific methods
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "X-Requested-With",
      "Accept",
    ], // Specify allowed headers
    preflightContinue: false,
    optionsSuccessStatus: 204,
  })
);
app.use(express.json());
app.options("*", cors()); // Enable preflight requests for all routes

const professorLocation = {
  latitude:  
  20.2190761,
  longitude: 85.7360815,
};

// Function to calculate distance using Lambert method
const calculateLambertDistanceInMeters = (loc1, loc2) => {
  const f = 1 / 298.257;
  let la1uLambert = (loc1.latitude * Math.PI) / 180;
  let la2uLambert = (loc2.latitude * Math.PI) / 180;

  if (Math.abs(loc1.latitude) < 90) {
    la1uLambert = Math.atan((1 - f) * Math.tan(la1uLambert));
  }
  if (Math.abs(loc2.latitude) < 90) {
    la2uLambert = Math.atan((1 - f) * Math.tan(la2uLambert));
  }

  const la1u = (loc1.latitude * Math.PI) / 180;
  const lo1u = (loc1.longitude * Math.PI) / 180;
  const la2u = (loc2.latitude * Math.PI) / 180;
  const lo2u = (loc2.longitude * Math.PI) / 180;

  const deltaLat = la2uLambert - la1uLambert;
  const deltaLon = lo2u - lo1u;

  const a =
    Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
    Math.cos(la1uLambert) *
      Math.cos(la2uLambert) *
      Math.sin(deltaLon / 2) *
      Math.sin(deltaLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  const P = (la1uLambert + la2uLambert) / 2;
  const Q = (la2uLambert - la1uLambert) / 2;

  const X =
    ((c - Math.sin(c)) *
      Math.sin(P) *
      Math.sin(P) *
      Math.cos(Q) *
      Math.cos(Q)) /
    Math.cos(c / 2) /
    Math.cos(c / 2);

  const Y =
    ((c + Math.sin(c)) *
      Math.sin(Q) *
      Math.sin(Q) *
      Math.cos(P) *
      Math.cos(P)) /
    Math.sin(c / 2) /
    Math.sin(c / 2);

  const distanceInMeters = 6378.1 * 1000 * (c - (f * (X + Y)) / 2);

  return distanceInMeters;
};

app.post("/send_request", (req, res) => {
  const { professor_id, student_ids } = req.body;
  console.log(
    `Professor ${professor_id} is sending requests to students: ${student_ids}`
  );

  student_ids.forEach((student_id) => {
    io.to(student_id).emit("request_message", {
      message: "Please mark your attendance",
      professor_id,
    });
  });

  res.json({ status: "Requests sent" });
});

app.post("/mark_attendance", (req, res) => {
  let { student_id, status, location } = req.body;
  console.log(
    `Student ${student_id} marked as ${status} with location ${JSON.stringify(
      location
    )}`
  );

  // Calculate distance
  const studentDistance = calculateLambertDistanceInMeters(
    professorLocation,
    location
  );
  console.log(`Distance: ${studentDistance} meters`);

  // Check if the student is within 30 meters
  if (studentDistance <= 30) {
    status = "Present";
  } else {
    status = "Absent";
  }

  // Create the attendance entry
  const attendanceEntry = {
    userId: student_id,
    attendance: status,
    date: new Date().toISOString(),
  };

  // Path to the JSON file
  const filePath = path.join(__dirname, "attendance.json");

  // Read the file and handle empty or invalid JSON
  fs.readFile(filePath, "utf8", (err, data) => {
    let json = [];

    if (err && err.code !== "ENOENT") {
      console.error("Error reading file", err);
      res.status(500).json({ status: "Error reading file" });
      return;
    }

    try {
      if (data) {
        json = JSON.parse(data);
      }
    } catch (e) {
      console.error("Invalid JSON in file", e);
    }

    json.push(attendanceEntry);

    fs.writeFile(filePath, JSON.stringify(json, null, 2), (err) => {
      if (err) {
        console.error("Error writing to file", err);
        res.status(500).json({ status: "Error saving attendance" });
        return;
      }
      res.json({ status: "Attendance marked", student_id, status });
    });
  });
});

io.on("connection", (socket) => {
  console.log(`Student connected: ${socket.id}`);

  socket.on("join", (student_id) => {
    socket.join(student_id);
    console.log(`Student ${student_id} joined`);
  });

  socket.on("disconnect", () => {
    console.log("Student disconnected");
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});