import { loginUserActionCreator } from "../redux/features/robots/userSlice";
import { useAppDispatch } from "../redux/hooks";
import { UserCredentials } from "../types";
import decodeToken from "../utils/decodeToken";

const useUser = () => {
  const dispatch = useAppDispatch();

  const url = process.env.REACT_APP_API_ROBOTS!;

  const getUserToken = async (userData: UserCredentials) => {
    const responseData = await fetch(`${url}/login`, {
      method: "POST",
      body: JSON.stringify(userData),
      headers: {
        "Content-type": "application/json; charset=UTF-8",
      },
    });

    const { token } = await responseData.json();

    const userLogged = decodeToken(token);

    dispatch(loginUserActionCreator({ ...userLogged, token }));
    localStorage.setItem("token", token);
  };

  return { getUserToken };
};

export default useUser;