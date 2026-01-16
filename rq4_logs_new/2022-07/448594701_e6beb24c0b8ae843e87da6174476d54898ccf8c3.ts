import axios from "axios";

export async function getInfo(API_LINK: string, KEY: string, username: string) {
  try {
    const method = "user.getinfo";
    const data = await axios.get(API_LINK, {
      params: {
        method: method,
        username,
        api_key: KEY,
        format: "json",
      },
    });
    const ProfileInfo = await data.data.user;
    console.log(ProfileInfo);
    return ProfileInfo;
  } catch (error) {
    console.error(error);
  }
}