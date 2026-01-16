import {
  AuthToken,
  AuthUseCase,
  Credentials,
} from "../entities/usecases/authUseCase";
import { HttpRepository } from "../repositories";

export class AuthService implements AuthUseCase {
  private gitAuth: HttpRepository;
  private oAuthToken?: AuthToken;

  constructor(gitAuth: HttpRepository) {
    this.gitAuth = gitAuth;
  }

  public setOAuthToken = async (credentials: Credentials) => {
    this.oAuthToken = await this.gitAuth.post<AuthToken>(
      "/access_token",
      credentials,
      {
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
      }
    );
  };

  public getOAuthHeader = () => {
    return {
      headers: { authorization: `Bearer ${this.oAuthToken?.access_token}` },
    };
  };
}