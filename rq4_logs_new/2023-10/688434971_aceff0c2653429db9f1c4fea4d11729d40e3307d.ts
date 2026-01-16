import {SocialUser} from "@abacritt/angularx-social-login";

export interface UserInterface {
  provider: string;
  id: string;
  email: string;
  name: string;
  photoUrl: string;
  firstName: string;
  lastName: string;
  authToken: string;
  idToken: string;
  authorizationCode: string;
  response: any;

  // custom fields
  subscription: 'Basic' | 'Pro';
}

export function mapSocialUserToUserInterface(user: SocialUser): UserInterface {
  return {
    provider: user.provider,
    id: user.id,
    email: user.email,
    name: user.name,
    photoUrl: user.photoUrl,
    firstName: user.firstName,
    lastName: user.lastName,
    authToken: user.authToken,
    idToken: user.idToken,
    authorizationCode: user.authorizationCode,
    response: user.response,

    // custom fields with basic values
    subscription: 'Basic'
  }
}