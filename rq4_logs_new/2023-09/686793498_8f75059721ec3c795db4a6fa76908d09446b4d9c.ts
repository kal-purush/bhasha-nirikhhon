import UserRepo from "@src/repos/UserRepo";

import PwdUtil from "@src/util/PwdUtil";
import { tick } from "@src/util/misc";

import HttpStatusCodes from "@src/constants/HttpStatusCodes";
import { RouteError } from "@src/other/classes";
import { IUser } from "hive-link-common";
import { User } from "@src/models/User";
import { INewUser, ISessionUser } from "hive-link-common";
import SessionUtil from "@src/util/SessionUtil";
import { IReq } from "@src/routes/types/types";
import { AppDataSource } from "@src/data-source";

const userRepo = AppDataSource.getRepository(User);

export function isSessionUser(user: any): user is ISessionUser {
  return (
    user &&
    typeof user.id === "number" &&
    typeof user.email === "string" &&
    typeof user.name === "string" &&
    typeof user.role === "number" &&
    typeof user.phone === "string"
  );
}

// **** Variables **** //

// Errors
export const Errors = {
  Unauth: "Unauthorized",
  EmailNotFound(email: string) {
    return `User with email "${email}" not found`;
  },
} as const;

// **** Functions **** //

async function me(req: IReq): Promise<ISessionUser> {
  var user: ISessionUser = {
    id: 0,
    email: "",
    firstName: "",
    lastName: "",
    role: 1,
  };
  await SessionUtil.getSessionData<ISessionUser>(req).then(
    (_user: ISessionUser | undefined | string) => {
      const { id, email, firstName, lastName, role } = _user as ISessionUser;
      user = { id, email, firstName, lastName, role } as ISessionUser;
      console.log(user);
      return user as ISessionUser;
    }
  );

  return user as ISessionUser;
}
/**
 * Login a user.
 */
async function login(email: string, password: string): Promise<IUser> {
  // Fetch user
  return userRepo.findOneBy({ email }).then((user) => {
    if (!user) {
      throw new RouteError(
        HttpStatusCodes.NOT_FOUND,
        Errors.EmailNotFound(email)
      );
    }
    const hash = user.authHash ?? "",
      pwdPassed = PwdUtil.compare(password, hash).then((res) => {
        if (!res) {
          // If password failed, wait 500ms this will increase security
          tick(500);
          throw new RouteError(HttpStatusCodes.UNAUTHORIZED, Errors.Unauth);
        }
      });
    return user;
  });
}

async function register(newUser: INewUser): Promise<User> {
  try {
    // Check if a user with the same email already exists
    const existingUser = await userRepo.findOne({
      where: { email: newUser.email },
    });
    if (existingUser) {
      throw new RouteError(
        HttpStatusCodes.CONFLICT,
        `User with email "${newUser.email}" already exists`
      );
    }

    // Create and save the new user
    const createdAt = new Date().toISOString().slice(0, 19).replace("T", " ");
    const user = userRepo.create({
      ...newUser,
      authHash: await PwdUtil.getHash(newUser.password),
      role: 0,
      createdAt: createdAt,
      updatedAt: createdAt,
      telephone: null,
    });

    await userRepo.save(user);
    return user;
  } catch (error) {
    throw error;
  }
}

// **** Export default **** //

export default {
  login,
  register,
  me,
} as const;