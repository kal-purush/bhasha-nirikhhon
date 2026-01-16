import { useContext } from "react";
import { AuthContext, AuthContextData } from "../context/AuthContext";

const useAuth = (): AuthContextData => {
  const authContext = useContext(AuthContext);
  return authContext;
};

export default useAuth;