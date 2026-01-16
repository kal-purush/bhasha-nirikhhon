import {
  COOKIE_NAME_ACCESS_TOKEN,
  COOKIE_NAME_REFRESH_TOKEN,
} from "@/constants";
import { cookiesApi } from "@/lib/js-cookie";
import { signInWithEmailAndPassword, signOut } from "firebase/auth";
import { auth } from "../config";

export const login = async (email: string, password: string) => {
  const userCredential = await signInWithEmailAndPassword(
    auth,
    email,
    password
  );
  const accessToken = await userCredential.user.getIdToken();
  cookiesApi.set(COOKIE_NAME_ACCESS_TOKEN, accessToken);
  cookiesApi.set(COOKIE_NAME_REFRESH_TOKEN, userCredential.user.refreshToken);
};

export const logout = async () => {
  await signOut(auth);
  cookiesApi.remove(COOKIE_NAME_ACCESS_TOKEN);
  cookiesApi.remove(COOKIE_NAME_REFRESH_TOKEN);
};