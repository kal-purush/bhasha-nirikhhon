import { AuthUseCase, Credentials, Position, User, UserCredential, UserUseCase } from "@domain/entities";
import { HttpRepository, LocalStorageRepository } from "@domain/repositories";
import { GitRepository, GitUser } from "@infrastructure/dto";
import { HttpRepositoryImp } from "@infrastructure/repositories";
import { GITHUB_URL } from "src/constants";
import { AuthContextProviderProps, UserContextProps } from "src/context";

export const FAKE_TOKEN = "github_fake_token";
export const USER_ID = 1234;
export const USER_EMAIL = "user@email.com";
export const GEOHASH = "eqwe132";
export const USERNAME = "username";
export const PHOTO_URL = "photo/url";
export const PROFILE_URL = "profile/url";

const AUTH_SESSION = {
  type: "success",
  params: {
    code: "any_value",
  },
};

export const POSITION = {
  latitude: 37.09739685,
  longitude: -24.55375671,
} as Position;

const geohashGeneratorMock = jest.fn(() => GEOHASH);

export const UserCredentialStub = {
  operationType: "signIn",
  providerId: 123443124,
  user: {
    email: USER_EMAIL,
    photoURL: PHOTO_URL,
    emailVerified: true,
  },
} as unknown as UserCredential;

class AuthServiceStub implements AuthUseCase {
  getOAuthHeader = () => ({ headers: { authorization: FAKE_TOKEN } });
  setOAuthToken = jest.fn();
  getOAuthToken = () => FAKE_TOKEN;
  signInWithCredentials = async () => Promise.resolve(UserCredentialStub);
}

const GithubUserStub = {
  items: [{
    id: USER_ID,
    avatar_url: PHOTO_URL,
    html_url: PROFILE_URL,
    login: USERNAME,
  }]
} as GitUser

export const GitRepositoryStub = [
  { language: "C" },
  { language: "JavaScript" },
  { language: "Java" },
  { language: "C++" },
] as GitRepository[];

class LocalStorageMock implements LocalStorageRepository {
  private value: any = {}

  setItem = jest.fn(async (key:string, value: any) => {
    this.value = value
  });

  removeItem = jest.fn(async () => {
    this.value = {}
  });

  getItem = jest.fn(async () => Promise.resolve<any>(this.value as any));
};

class UserServiceStub implements UserUseCase {
  private user = {} as User
  
  createUser = jest.fn(async (user:User) => {
    this.user = user
  })

  getUser =  jest.fn(async () => this.user)
  listUsers = jest.fn(async () => [this.user])
} ;

class GithubApiStub implements HttpRepositoryImp {
  api = jest.fn()

  async get<T>(endpoint: string) {
    if(endpoint.includes('/search/users')) return Promise.resolve<T>(GithubUserStub as T);
    if(endpoint.includes('/repos')) return Promise.resolve<T>(GitRepositoryStub as T);
    return undefined;
  }

  async post<T>(endpoint: string): Promise<T | undefined> {
    if(endpoint.includes('/access_token')) return Promise.resolve<T>(FAKE_TOKEN as T);
  }
}

const localStorage = new LocalStorageMock()
const githubApi = new GithubApiStub()
const userService = new UserServiceStub()
const authService = new AuthServiceStub()

export const userContexProps = {
  geohashGenerator: geohashGeneratorMock,
  localStorage,
  authService,
  userService,
  githubApi,
} as UserContextProps;

export const authContexProps = {
  promptAsync: jest.fn(async () => AUTH_SESSION),
  localStorage,
  authService,
} as AuthContextProviderProps;