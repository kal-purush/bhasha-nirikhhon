import { IUserObject } from "../Types/Interfaces/Login.types";

export const userObject: IUserObject = {
  username: {
    value: "",
    fieldName: "username",
    valid: true,
    required: true,
    label: "User Name",
  },
  password: {
    value: "",
    fieldName: "password",
    valid: true,
    required: true,
    label: "Password",
  },
};