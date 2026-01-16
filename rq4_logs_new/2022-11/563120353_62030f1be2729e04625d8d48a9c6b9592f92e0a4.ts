import { AuthUseCase, Position, User, UserCredential, UserUseCase } from "@domain/entities";
import { HttpRepository, LocalStorageRepository } from "@domain/repositories";
import { GitRepository, GitUser } from "@infrastructure/dto";
import { AuthContextProviderProps } from "src/context";

export const FAKE_TOKEN = "github_fake_token";
export const USER_ID = 1234;
export const USER_EMAIL = "user@email.com";
export const GEOHASH = "eqwe132";
export const USERNAME = "username";
export const PHOTO_URL = "photo/url";
export const PROFILE_URL = "profile/url";

export const AUTH_SESSION = {
  type: "success",
  params: {
    code: "any_value",
  },
};

export const POSITION = {
  latitude: 37.09739685,
  longitude: -24.55375671,
} as Position;

export const geohashGeneratorMock = jest.fn(() => GEOHASH);

export const UserCredentialStub = {
  operationType: "signIn",
  providerId: 9876,
  user: {
    email: USER_EMAIL,
    photoURL: PHOTO_URL,
  },
} as unknown as UserCredential;

export const AuthServiceStub = {
  getOAuthHeader: () => ({
    headers: {
      authorization: FAKE_TOKEN,
    },
  }),
  getOAuthToken: () => FAKE_TOKEN,
  setOAuthToken: jest.fn(),
  signInWithCredentials: jest.fn(async () => UserCredentialStub),
} as AuthUseCase;

export const GithubApiStub = {
  get: jest.fn(async (url: string) => {
    if (url.includes(USER_EMAIL)) return GitUserStub;
    return GitRepositoryStub;
  }),
  post: jest.fn(async (url, data) => {
    return UserStub;
  }),
} as HttpRepository;

export const GitUserStub = {
  items: [
    {
      email: USER_EMAIL,
      id: USER_ID,
      html_url: PROFILE_URL,
      login: USERNAME,
      avatar_url: PHOTO_URL,
    },
  ],
} as GitUser;

export const GitRepositoryStub = [
  { language: "C" },
  { language: "JavaScript" },
  { language: "Java" },
  { language: "C++" },
] as GitRepository[];

export const UserStub = {
  geohash: GEOHASH,
  email: USER_EMAIL,
  username: USERNAME,
  id: USER_ID,
  techs: GitRepositoryStub.map((tech) => tech.language),
  photoUrl: PHOTO_URL,
  profileUrl: PROFILE_URL,
  position: POSITION,
} as User;

export const LocalStorageStub = {
  setItem: jest.fn(async () => {}),
  getItem: jest.fn(async () => {}),
  removeItem: jest.fn(async () => {}),
} as LocalStorageRepository;

export const UserServiceStub = {
  createUser: jest.fn(async () => undefined),
  getUser: jest.fn(async () => UserStub),
  listUsers: jest.fn(async () => [UserStub]),
} as UserUseCase;

export const userContexDefaultProps = {
  authService: AuthServiceStub,
  geohashGenerator: geohashGeneratorMock,
  githubApi: GithubApiStub,
  localStorage: LocalStorageStub,
  userService: UserServiceStub,
};

export const authContexDefaultProps = {
  authService: AuthServiceStub,
  localStorage: LocalStorageStub,
  promptAsync: jest.fn(async () => AUTH_SESSION),
} as Omit<AuthContextProviderProps, "children">;