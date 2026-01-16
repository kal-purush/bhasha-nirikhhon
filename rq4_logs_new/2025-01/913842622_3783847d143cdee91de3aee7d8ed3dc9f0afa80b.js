const jwt = require("jsonwebtoken");

const JWT_SECRET = "your_jwt_secret";

const verifyToken = (req, res, next) => {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(403).json({ message: "No token provided" });
  }

  jwt.verify(token, JWT_SECRET, (err, decoded) => {
    if (err) {
      return res.status(500).json({ message: "Failed to authenticate token" });
    }

    // Save the user ID for use in other routes
    req.userId = decoded.userId;
    next();
  });
};

module.exports = verifyToken;