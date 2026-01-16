const config = require("./config");
const jwt = require("jsonwebtoken");

module.exports = (req, res, next) => {
  const userToken = req.headers["user-token"];
  if (!userToken) return res.status(401).json({ Error: "No Header Token" });
  const decoded = config.verifyToken(userToken);
  if (
    decoded.userName == req.body.userName ||
    decoded.userName == req.params.userName
  )
    next();
  else return res.status(401).json({ Error: "Invalid Token" });
};