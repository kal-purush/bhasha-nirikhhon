import { RegisterVerificationInterface } from "../interfaces";
import sendEmail from "./sendEmail";

const registerEmail = async ({
  userEmail,
  userName,
  verificationToken,
}: RegisterVerificationInterface) => {
  try {
    const clientAddress = process.env.CLIENT_ADDRESS as string;
    const clientLink = `${clientAddress}/verify-email?verificationToken=${verificationToken}&email=${userEmail}`;
    const html = `<h1>HELLO ${userName}</h1>\n<p>Please click this link to verify your account: <a href=${clientLink}>click here</a></p>`;
    const subject = "User Verification";

    return sendEmail({
      to: userEmail,
      subject,
      html,
    });
  } catch (error) {
    console.log(error);
  }
};

export default registerEmail;