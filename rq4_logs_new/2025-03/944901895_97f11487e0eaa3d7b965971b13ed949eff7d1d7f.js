
const UserModel = require("../models/UserModel");
const jwt = require("jsonwebtoken");

const { oAuth2Client} = require("../utills/googleconfig");

const googleLogin = async (req, res) => {
  try {
    const { code } = req.body;
    console.log("Received authorization code:", code);

    const { tokens } = await oAuth2Client.getToken(code);
    const accessToken = tokens.access_token;
    const idToken = tokens.id_token;

    const ticket = await oAuth2Client.verifyIdToken({
      idToken: idToken,
      audience: process.env.GOOGLE_CLIENT_ID,
    });
    const payload = ticket.getPayload();
    const { email, name, picture, sub: googleId } = payload;

    let user = await UserModel.findOne({ email });
    if (!user) {
      user = await UserModel.create({
        name,
        email,
        password: "GOOGLE_OAUTH",
        image: picture,
        googleAccessToken: accessToken,
      });
    } else {
      user.googleAccessToken = accessToken;
      await user.save();
    }

    const jwtToken = jwt.sign(
      { _id: user._id, email },
      process.env.JWT_SECRET,
      { expiresIn: "1h" }
    );

    res.cookie("authToken", jwtToken, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "strict",
      maxAge: 3600000, // 1 hour
    });

    return res.status(200).json({
      message: "Success",
      token: jwtToken,
      user: {
        id: user._id,
        name: user.name,
        email: user.email,
        image: user.image,
      },
    });
  } catch (err) {
    console.error("Google login error:", err);
    return res.status(500).json({ message: "Internal Server Error", error: err.message });
  }
};

module.exports = {
  googleLogin,
};