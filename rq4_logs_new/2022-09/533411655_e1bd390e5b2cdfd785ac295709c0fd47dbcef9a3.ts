import { useNavigate } from "react-router-dom";
import { post } from "Utils/api";

interface IPostLogin {
  email: string;
  password: string;
}

interface IPostSignUp {
  userName: string;
  nickName: string;
  email: string;
  password: string;
}

export const useAuth = () => {
  const navigate = useNavigate();
  // 회원가입
  const postSignUp = async ({
    userName,
    nickName,
    email,
    password,
  }: IPostSignUp) => {
    const res = await post(`/user/post`, {
      userName,
      nickName,
      email,
      password,
    }).then((r: any) => {
      console.log(r);
    });
    return { res };
  };

  const postLogin = async ({ email, password }: IPostLogin) => {
    const res = await post(`/login`, { email, password }).then((r: any) =>
      console.log(r),
    );
    return { res };
  };
  return {
    postSignUp,
    postLogin,
  };
};