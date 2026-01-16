
import { Auth } from 'aws-amplify';

/* signUp user */
export const signUp = async (email: string, password: string) => {
  return await Auth.signUp(email, password);
}

/* signUp user */
export const signUpWithEmail = async (body: any) => {
  return await Auth.signUp(body);
}

/* Confirm signUp user */
export const confirmSignUp = async (email: string, code: string) => {
  return await Auth.confirmSignUp(email, code);
}

/* login user */
export const login = async (email: string, password: string) => {
  return await Auth.signIn(email, password);
}

/* logout user */
export const logout = async () => {
  return await Auth.signOut();
}
