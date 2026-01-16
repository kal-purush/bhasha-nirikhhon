const jwt = require("jsonwebtoken");

const { verify } = require("jsonwebtoken");

const auth = (req, res, next) => {
  try {
    var token = req.headers["auth-token"];

    if (!token) {
      return res.status(401).json({ msg: "user is not authenticated" });
    }

    const verify = jwt.verify(token, "passwordKey");

    if (!verify) return res.status(401).json({ msg: "token is not verified" });

    req.user = verify.id;
    req.token = token;
    next();
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};

module.exports = auth;