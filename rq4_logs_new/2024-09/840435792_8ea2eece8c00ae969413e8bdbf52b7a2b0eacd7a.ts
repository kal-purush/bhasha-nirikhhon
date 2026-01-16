

import { authUrl } from "@/apis/auth";
import { makeApiRequest } from "@/apis/functions";
import {userSignup, userLogin} from '../../../../interfaces/users'
//userLogin 
export const handleLogin = async ({ email, password}:userLogin) => {
    try {
      const response = await makeApiRequest({
        method: "POST",
        url: authUrl.USER_LOGIN,
        data: { email, password },
      });
      console.log("Login successful:", response);
      
    } catch (error) {
      console.error("Login failed:", error);
    
    }
  };

export const handleSignup = async ({firstName, lastName, email, password }:userSignup) => {
    try {
      const response = await makeApiRequest({
        method: "POST",
        url: authUrl.USER_SIGNUP,
        data: {firstName, lastName, email, password },
      });
      console.log("Login successful:", response);
      
    } catch (error) {
      console.error("Login failed:", error);
    
    }
  };