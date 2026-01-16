export enum AuthState {
  SIGN_IN = "SIGN_IN",
  SIGN_UP = "SIGN_UP",
}
export type FieldType = {
  labelText: string;
  id: string;
  name: string;
  type: string;
  isRequired: boolean;
  authState?: AuthState;
};

export const fields = [
  {
    labelText: "Name",
    id: "name",
    name: "name",
    type: "text",
    isRequired: true,
    authState: AuthState.SIGN_UP,
  },
  {
    labelText: "Email address",
    id: "email",
    name: "email",
    type: "email",
    isRequired: true,
  },
  {
    labelText: "Password",
    id: "password",
    name: "password",
    type: "password",
    isRequired: true,
  },
];