const express = require("express");
const router = express.Router();

router.get("/", (req, res, next) => {
  res.send("running glitch api");
});
module.exports = router;