const { User } = require("../../models/user");
const { PORT = 3000 } = process.env;
const { generationError, sendEmail } = require("../../helpers");
const resendVerifyEmail = async (req, res) => {
  const { email } = req.body;
  if (!email) {
    throw generationError(400, "missing required field email");
  }
  const user = await User.findOne({ email });
  console.log(user);
  if (!user) {
    throw generationError(404, "Not found");
  }
  if (user.verify) {
    throw generationError(400, "Verification has already been passed");
  }
  const mail = {
    to: email,
    subject: "повторне підтвердження реєстрації",
    html: `<a href="http://localhost:${PORT}/api/auth/users/verify/${user.verificationToken}" 
    target="_blank" >Перейдіть за посилання для підтвердження реєстрації
    <br>http://localhost:${PORT}/api/auth/users/verify/${user.verificationToken}</a>`,
  };
  await sendEmail(mail);
  res.status(200).json({ message: "Verification email sent" });
};
module.exports = resendVerifyEmail;