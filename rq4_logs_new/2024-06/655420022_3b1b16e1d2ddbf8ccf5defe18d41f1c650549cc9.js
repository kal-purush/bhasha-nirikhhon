const { User, Token, Session } = require("../../models/user");
const { HttpError, ctrlWrapper } = require("../../helpers");

const jwt = require("jsonwebtoken");

const { SECRET_KEY, REFRESH_SECRET_KEY } = process.env;

const refreshUser = async (req, res) => {
  const email = req.body.email;

  if (!email || !req.body.tokenRefresh) {
    throw HttpError(400, "Error. Provide all required fields");
  }

  const userRefresh = await Token.findOne({ email: email });

  if (!userRefresh) {
    throw HttpError(401, "User email invalid or unauthorized");
  }

  const validateTokenResult = jwt.verify(
    req.body.tokenRefresh,
    REFRESH_SECRET_KEY
  );

  if (
    req.body.tokenRefresh !== userRefresh.tokenRefresh ||
    !validateTokenResult
  ) {
    await User.findOneAndUpdate({ email }, { token: "" });
    await Token.findOneAndRemove({ email });
    throw HttpError(403, "Refresh token invalid or unauthorized");
  }

  const user = await User.findOne({ email: email });

  if (!user) {
    throw HttpError(401, "User email invalid or unauthorized");
  }

  const newSession = await Session.create({
    uid: user._id,
  });

  const payload = {
    id: user._id,
    sid: newSession._id,
  };

  const token = jwt.sign(payload, SECRET_KEY, { expiresIn: "11h" });
  const tokenRefresh = jwt.sign(payload, REFRESH_SECRET_KEY, {
    expiresIn: "23d",
  });

  await User.findByIdAndUpdate(user._id, { token });
  await Token.findOneAndUpdate({ email }, { tokenRefresh });

  res.status(200);
  res.json({
    code: 200,
    message: "Refresh success",
    token,
    tokenRefresh,
  });
};

module.exports = {
  refreshUser: ctrlWrapper(refreshUser),
};