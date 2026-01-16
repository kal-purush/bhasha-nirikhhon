import { useMutation } from "@apollo/client";
import { useContext } from "react";
import { BBRRPClientContext } from "../context";
import { AUTH_MUTATION } from "../graphql/auth";
import {
  AuthMutation,
  AuthMutationVariables,
} from "../interfaces/AuthMutation";
import { isLoginSuccess } from "../shared/helpers";

export const useLogin = () => {
  const { setLoggedIn } = useContext(BBRRPClientContext);
  const [authMutation, { loading, data: result }] = useMutation<
    AuthMutation,
    AuthMutationVariables
  >(AUTH_MUTATION);

  const login = async (email: string, password: string) => {
    const res = await authMutation({
      variables: {
        email,
        password,
      },
    });
    const success = isLoginSuccess(res?.data);
    if (success) {
      setLoggedIn(true);
    } else {
      alert("Login failed");
    }
  };

  const logout = () => {
    setLoggedIn(false);
  };

  return {
    login,
    logout,
    loading,
    success: isLoginSuccess(result),
  };
};