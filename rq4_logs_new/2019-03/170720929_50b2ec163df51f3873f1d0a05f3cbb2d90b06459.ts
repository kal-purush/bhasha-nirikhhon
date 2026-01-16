import { User } from "../utility/types";

const API = process.env.REACT_APP_SOCIAL_API_URL;
const USER = "user";

export default class SocialApiRepository {
  /**
   * Updates the user information in the database.
   * @param accessToken Token for authentication.
   * @param user User data to update.
   */
  async updateUserInfo(accessToken: string, user: User) {
    return await this.getInjectedFetch(API + USER, accessToken, "PUT", user);
  }

  /**
   * Injects a fetch object with access token headers and returns it.
   * @param url Path to query.
   * @param accessToken Access token to verify users.
   */
  private getInjectedFetch(
    url: string,
    accessToken: string | null,
    queryType: string = "GET",
    body: {} | null = null
  ) {
    var headers = new Headers({
      "content-type": "application/json",
      Authorization: "Bearer " + accessToken
    });

    return accessToken != null
      ? fetch(url, {
          headers: headers,
          method: queryType,
          body: body != null ? JSON.stringify(body) : null
        })
      : fetch(url, {
          method: queryType,
          body: body != null ? JSON.stringify(body) : null
        });
  }
}