import { OAuth } from "./oauth";

export interface MicrosoftAuthConfig {
  /** Azure AD App Registration App ID */
  client_id: string;
  /** A redirect url you configured in the Azure AD App registration. Defaults to the root of your app */
  redirect_uri?: string;
  /** Defaults to User.Read */
  scope?: string;
}

export interface UserProfile {
  displayName: string;
  givenName: string;
  jobTitle: string;
  mail: string;
  mobilePhone: string;
  officeLocation: string;
  preferredLanguage?: any;
  surname: string;
  userPrincipalName: string;
  id: string;
}
const cacheKeys = {
  CURRENT_USER: "microsoft-current-user",
};

export class MicrosoftAuth extends OAuth {
  constructor(config: MicrosoftAuthConfig) {
    super({
      authorizeEndpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
      tokenEndpoint: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
      scope: "User.Read",
      redirect_uri: location.origin,
      ...config,
    });
  }

  public get currentUser() {
    try {
      return JSON.parse(sessionStorage.getItem(cacheKeys.CURRENT_USER));
    } catch (err) {
      return null;
    }
  }

  logout = () => {
    super.logout();
    sessionStorage.setItem(cacheKeys.CURRENT_USER, "");
  };

  ensureLogin = async (): Promise<UserProfile> => {
    let oauthResult = await this.ensureToken();
    let user = this.currentUser;

    if (oauthResult?.access_token && !user) {
      user = await fetchCurrentUser(oauthResult.access_token);
      if (user) {
        sessionStorage.setItem(cacheKeys.CURRENT_USER, JSON.stringify(user));
      }
    }

    return user;
  };
}

const fetchCurrentUser = function (token): Promise<UserProfile> {
  const url = "https://graph.microsoft.com/v1.0/me/";
  return fetch(url, {
    headers: {
      accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
  }).then((resp) => resp.json());
};