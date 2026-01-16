import { useEffect, useRef } from "react";
import { IAuth, ITokens } from "../interfaces/ITokens";
import IUser from "../interfaces/IUser";

const defaultTokens: ITokens = {
  access: { token: "", expires: new Date() },
  refresh: { token: "", expires: new Date() },
};

const defaultUser: IUser = {
  id: "",
  username: "",
  email: "",
  password: "",
  isEmailVerified: false,
  role: "player",
};

const usePreviousAuth = (value: IAuth) => {
  const ref = useRef<IAuth>({
    user: defaultUser,
    tokens: defaultTokens,
  });

  useEffect(() => {
    ref.current = value; //assign the value of ref to the argument
  }, [value]); //this code will run when the value of 'value' changes
  return ref.current; //in the end, return the current ref value.
};
export default usePreviousAuth;