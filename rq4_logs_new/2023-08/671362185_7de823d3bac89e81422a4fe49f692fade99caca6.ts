import axios from "axios";

async function CSRFToken(jenkinsUrl: any, username: any, password: any) {
  try {
    const crumbUrl = `${jenkinsUrl}crumbIssuer/api/json`;
    const response = await axios.get(crumbUrl, {
      auth: {
        username,
        password,
      },
    });

    const csrfToken = response.data.crumb
    console.log(csrfToken);
    return csrfToken;
  } catch (error:any) {
    console.error(`Erorr generating token:`, error.message)
    throw error
  }
}

export default CSRFToken ;