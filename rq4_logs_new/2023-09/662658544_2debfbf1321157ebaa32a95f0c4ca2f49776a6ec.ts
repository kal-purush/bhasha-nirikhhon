/**
 * @summary Definition of the User class.
 * @author Bojun Ren
 * @data 2023/08/28
 */

import { type Badge } from "./Badge";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { storageName } from "./config";

/**
 * The canonical response POJO of the user information.
 * @alias LoginResponse
 * @alias UserProfileResponse
 */
export interface UserInfoPOJO {
  username: string;
  wallet: string;
  avatar: string;
  signature: string;
  muted: boolean;
  banned: boolean;
  balance: number;
  credibility: number;
  privilege: number;
  activeRole: string;
  rolesAssigned: string[];
  activeBadge: Badge;
  badgesReceived: Badge[];
  createdAt: Date;
  updateAt: Date;
}

/**
 * Convert a json object to the UserInfoPOJO
 * @param json The json object
 * @returns UserInfoPOJO
 */
export function jsonToUserInfoPOJO(
  json: Record<string, unknown>
): UserInfoPOJO {
  const res = {} as UserInfoPOJO;
  Object.assign(res, json);
  res.createdAt = new Date(json.createdAt as string);
  res.updateAt = new Date(json.updatedAt as string);
  return res;
}

type _UserInfo = UserInfoPOJO | null;
export class User {
  /** Locally stored information */
  /** Negative user_id indicates the entity isn't persisted. */
  private user_id = -1;
  private readonly privateKey: string;
  private readonly cert: string;

  /** Ephemeral data obtained from the backend */
  private _profile: Readonly<_UserInfo> = null;

  public constructor(privateKey: string, cert: string);

  public constructor(privateKey: string, cert: string) {
    this.privateKey = privateKey;
    this.cert = cert;
  }

  public set profile(userInfo: _UserInfo) {
    this._profile = userInfo;
  }
  public get profile(): _UserInfo {
    return this._profile;
  }

  /**
   * Get the number of users stored locally.
   */
  public static async getUserCount(): Promise<number> {
    const userCount = parseInt(
      (await AsyncStorage.getItem(storageName.userCount)) as string
    );
    return userCount;
  }

  /**
   * Persist the current user entity.
   * The `privateKey` and `cert` properties must be defined.
   */
  public async persist(): Promise<void> {
    const userCount = await User.getUserCount();
    if (Number.isNaN(userCount)) {
      throw `Can't parse the ${storageName.userCount} key`;
    }
    await Promise.all([
      AsyncStorage.setItem(
        storageName.userId(userCount),
        JSON.stringify({ privateKey: this.privateKey, cert: this.cert })
      ),
      AsyncStorage.setItem(storageName.userCount, (userCount + 1).toString()),
    ]);
    this.user_id = userCount;
  }

  /**
   * Get the user stored locally with the specified id.
   * @param id
   */
  public static async getUser(id: number): Promise<User> {
    const userCount = await User.getUserCount();
    if (id >= userCount || id < 0) {
      throw "Invalid id!";
    }
    const { cert, privateKey } = JSON.parse(
      (await AsyncStorage.getItem(storageName.userId(id))) as string
    ) as {
      cert: string;
      privateKey: string;
    };
    const resUser = new User(cert, privateKey);
    resUser.user_id = id;
    return resUser;
  }
}