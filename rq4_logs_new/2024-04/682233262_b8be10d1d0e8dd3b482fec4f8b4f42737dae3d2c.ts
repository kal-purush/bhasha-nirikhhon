import { Status } from "@/model/status";

export const handleSignupError = (error: Error): Status => {
  if (error.message === "User already registered") {
    return { type: "error", message: "이미 가입한 이메일 입니다." };
  } else {
    return { type: "error", message: error.message };
  }
};

export const handleSigninError = (error: Error): Status => {
  if (error.message === "Invalid login credentials") {
    return { type: "error", message: "이메일 혹은 비밀번호를 확인해주세요." };
  } else {
    return { type: "error", message: error.message };
  }
};

export const handleCreatePostError = (error: Error): Status => {
  return { type: "error", message: error.message };
};

export const handleFormError = (error: Error): Status => {
  return { type: "error", message: error.message };
};