import { getData } from "@/services/axios";
import { phoneSchema } from "@/validator/phone";
import alertErr from "@/validator/showError";
import Cookies from "js-cookie";
import React, { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "./context/useAuth";

const useLogin = (phoneNumber: string) => {
  const [errRes, setErrRes] = useState<string[]>();
  const navigate = useNavigate();
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  const from = params.get("from");
  const { setAuth } = useAuth();
  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    try {
      phoneSchema.parse(phoneNumber);
      const response = await getData("user");
      console.log(response);
      if (response.status === 200) {
        console.log("hello");
        Cookies.set("token", "authorization", {
          path: "/",
          expires: 0.5,
          secure: true,
          sameSite: "strict",
        });
        const firstUser = response.data.data.find((user: any) => user.id === 2);
        setAuth(firstUser);
        console.log(`/${firstUser.role}`, "aaaaaaaaaaaaaaaaaaaaaa");
        console.log({ from });
        navigate(from || `/${firstUser.role}`);
      }
    } catch (error) {
      const err = alertErr(error);
      setErrRes(err);
      setTimeout(() => setErrRes([]), 4000);
    }
  };

  return { errRes, handleSubmit };
};

export default useLogin;