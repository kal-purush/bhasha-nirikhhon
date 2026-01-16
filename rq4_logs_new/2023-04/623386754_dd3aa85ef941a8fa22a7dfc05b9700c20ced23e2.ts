import sendEmail from "./sendEmail";
import { RegisterVerificationInterface } from "../interfaces";
const forgotPasswordEmail = async ({
  userEmail,
  userName,
  verificationToken: passwordToken,
}: RegisterVerificationInterface) => {
  try {
    const clientAddress = process.env.CLIENT_ADDRESS as string;
    const clientLink = `${clientAddress}/forgot-password?passwordToken=${passwordToken}&email=${userEmail}`;
    const html = `<h1>HELLO ${userName}</h1>\n<p>Please click this link to reset your password: <a href=${clientLink}>click here</a></p>`;
    const subject = "Password Reset";

    return sendEmail({
      to: userEmail,
      subject,
      html,
    });
  } catch (error) {
    console.log(error);
  }
};

export default forgotPasswordEmail;