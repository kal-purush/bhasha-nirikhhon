import { getSession } from "./sessionHelper";

export const getTokenFromSession = async (tokenType: string) => {
  try {
    const session = await getSession();

    if (!session) {
      throw new Error("Session is missing.");
    } else if (!session.tokenForAPI) {
      throw new Error("Session token is missing.");
    }
    if (tokenType === "API") {
      return session.tokenForAPI;
    } else {
      return session.tokenForVerification;
    }
  } catch (error) {
    console.error(error);
    return null;
  }
};