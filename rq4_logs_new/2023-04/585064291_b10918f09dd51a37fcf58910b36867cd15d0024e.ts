import axios from "axios";

type verifyEmailAuthType = {
  email: string;
  authCode: string;
};

export const sendEmailAuth = async (email: string) => {
  const queryKey = "/api/v1/email/send-auth-code?_csrf=4606342a-7298-483f-8fc3-f7521673856f";
  const response = await axios.post(queryKey, { email });
  return response.data;
};

export const verifyEmailAuth = async ({ email, authCode }: verifyEmailAuthType) => {
  const queryKey = "/api/v1/email/valid";
  const response = await axios.post(queryKey, { email, authCode });
  return response.data;
};